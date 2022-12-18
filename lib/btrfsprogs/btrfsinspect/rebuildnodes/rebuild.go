// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	"sort"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildmappings"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/uuidmap"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type Rebuilder struct {
	raw   *btrfs.FS
	inner interface {
		btrfstree.TreeOperator
		Augment(treeID btrfsprim.ObjID, nodeAddr btrfsvol.LogicalAddr) ([]btrfsprim.Key, error)
	}
	sb btrfstree.Superblock

	graph        graph.Graph
	uuidMap      uuidmap.UUIDMap
	csums        containers.RBTree[containers.NativeOrdered[btrfsvol.LogicalAddr], btrfsinspect.SysExtentCSum]
	leaf2orphans map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]
	key2leaf     containers.SortedMap[keyAndTree, btrfsvol.LogicalAddr]

	augments map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]

	pendingAugments map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int
}

func RebuildNodes(ctx context.Context, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) (map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], error) {
	scanData, err := ScanDevices(ctx, fs, nodeScanResults) // ScanDevices does its own logging
	if err != nil {
		return nil, err
	}

	dlog.Info(ctx, "Reading superblock...")
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	dlog.Info(ctx, "Indexing checksums...")
	csums, _ := rebuildmappings.ExtractLogicalSums(ctx, nodeScanResults)
	if csums == nil {
		csums = new(containers.RBTree[containers.NativeOrdered[btrfsvol.LogicalAddr], btrfsinspect.SysExtentCSum])
	}

	dlog.Info(ctx, "Indexing orphans...")
	leaf2orphans, key2leaf, err := indexOrphans(ctx, fs, *sb, *scanData.nodeGraph)
	if err != nil {
		return nil, err
	}

	dlog.Info(ctx, "Rebuilding node tree...")
	o := &Rebuilder{
		raw:   fs,
		inner: btrfsutil.NewBrokenTrees(ctx, fs),
		sb:    *sb,

		graph:        *scanData.nodeGraph,
		uuidMap:      *scanData.uuidMap,
		csums:        *csums,
		leaf2orphans: leaf2orphans,
		key2leaf:     *key2leaf,

		augments: make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]),
	}
	if err := o.rebuild(ctx); err != nil {
		return nil, err
	}

	return o.augments, nil
}

func (o *Rebuilder) ioErr(ctx context.Context, err error) {
	err = fmt.Errorf("should not happen: i/o error: %w", err)
	dlog.Error(ctx, err)
	panic(err)
}

func (o *Rebuilder) rebuild(ctx context.Context) error {
	passNum := 0
	dlog.Infof(ctx, "... pass %d: scanning for implied items", passNum)
	o.pendingAugments = make(map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int)
	btrfsutil.WalkAllTrees(ctx, o.inner, btrfsutil.WalkAllTreesHandler{
		Err: func(*btrfsutil.WalkError) {},
		TreeWalkHandler: btrfstree.TreeWalkHandler{
			Item: func(path btrfstree.TreePath, item btrfstree.Item) error {
				handleItem(o, ctx, path[0].FromTree, item)
				return nil
			},
		},
	})
	for len(o.pendingAugments) > 0 {
		// Apply the augments, keeping track of what keys are added to what tree.
		dlog.Infof(ctx, "... pass %d: augmenting trees to add implied items", passNum)
		newKeys := make(map[btrfsprim.ObjID][]btrfsprim.Key)
		for _, treeID := range maps.SortedKeys(o.pendingAugments) {
			dlog.Infof(ctx, "... ... augmenting tree %v:", treeID)
			treeAugments := o.resolveTreeAugments(ctx, o.pendingAugments[treeID])
			for _, nodeAddr := range maps.SortedKeys(treeAugments) {
				added, err := o.inner.Augment(treeID, nodeAddr)
				if err != nil {
					dlog.Errorf(ctx, "error augmenting: %v", err)
					continue
				}
				newKeys[treeID] = append(newKeys[treeID], added...)

				set := o.augments[treeID]
				if set == nil {
					set = make(containers.Set[btrfsvol.LogicalAddr])
					o.augments[treeID] = set
				}
				set.Insert(nodeAddr)
			}
		}
		// Clear the list of pending augments.
		o.pendingAugments = make(map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int)
		passNum++
		// Call handleItem() for each of the added keys.
		dlog.Infof(ctx, "... pass %d: scanning for implied items", passNum)
		for _, treeID := range maps.SortedKeys(newKeys) {
			for _, key := range newKeys[treeID] {
				item, err := o.inner.TreeLookup(treeID, key)
				if err != nil {
					o.ioErr(ctx, fmt.Errorf("error looking up already-inserted item: tree=%v key=%v: %w",
						treeID, key, err))
				}
				handleItem(o, ctx, treeID, item)
			}
		}
	}
	return nil
}

func (o *Rebuilder) resolveTreeAugments(ctx context.Context, listsWithDistances []map[btrfsvol.LogicalAddr]int) containers.Set[btrfsvol.LogicalAddr] {
	distances := make(map[btrfsvol.LogicalAddr]int)
	generations := make(map[btrfsvol.LogicalAddr]btrfsprim.Generation)
	lists := make([]containers.Set[btrfsvol.LogicalAddr], len(listsWithDistances))
	for i, listWithDistances := range listsWithDistances {
		lists[i] = make(containers.Set[btrfsvol.LogicalAddr], len(listWithDistances))
		for item, dist := range listWithDistances {
			lists[i].Insert(item)
			distances[item] = dist
			generations[item] = o.graph.Nodes[item].Generation
		}
	}

	// Define an algorithm that takes several lists of items, and returns a
	// set of those items such that each input list contains zero or one of
	// the items from your return set.  The same item may appear in multiple
	// of the input lists.
	//
	// > Example 1: Given the input lists
	// >
	// >     0: [A, B]
	// >     2: [A, C]
	// >
	// > legal solutions would be `[]`, `[A]`, `[B]`, `[C]`, or `[B, C]`.  It
	// > would not be legal to return `[A, B]` or `[A, C]`.
	//
	// > Example 2: Given the input lists
	// >
	// >     1: [A, B]
	// >     2: [A]
	// >     3: [B]
	// >
	// > legal solution woudl be `[]`, `[A]` or `[B]`.  It would not be legal
	// > to return `[A, B]`.
	//
	// The algorithm should optimize for the following goals:
	//
	//  - We prefer that each input list have an item in the return set.
	//
	//    > In Example 1, while `[]`, `[B]`, and `[C]` are permissable
	//    > solutions, they are not optimal, because one or both of the input
	//    > lists are not represented.
	//    >
	//    > It may be the case that it is not possible to represent all lists
	//    > in the result; in Example 2, either list 2 or list 3 must be
	//    > unrepresented.
	//
	//  - Each item has a non-negative scalar "distance" score, we prefer
	//    lower distances.  Distance scores are comparable; 0 is preferred,
	//    and a distance of 4 is twice as bad as a distance of 2.
	//
	//  - Each item has a "generation" score, we prefer higher generations.
	//    Generation scores should not be treated as a linear scale; the
	//    magnitude of deltas is meaningless; only the sign of a delta is
	//    meaningful.
	//
	//    > So it would be wrong to say something like
	//    >
	//    >     desirability = (-a*distance) + (b*generation)       // for some constants `a` and `b`
	//    >
	//    > because `generation` can't be used that way
	//
	//  - We prefer items that appear in more lists over items that appear in
	//    fewer lists.
	//
	// The relative priority of these 4 goals is undefined; preferrably the
	// algorithm should be defined in a way that makes it easy to adjust the
	// relative priorities.

	ret := make(containers.Set[btrfsvol.LogicalAddr])
	illegal := make(containers.Set[btrfsvol.LogicalAddr]) // cannot-be-accepted and already-accepted
	accept := func(item btrfsvol.LogicalAddr) {
		ret.Insert(item)
		for _, list := range lists {
			if list.Has(item) {
				illegal.InsertFrom(list)
			}
		}
	}

	counts := make(map[btrfsvol.LogicalAddr]int)
	for _, list := range lists {
		for item := range list {
			counts[item] = counts[item] + 1
		}
	}

	sortedItems := maps.Keys(distances)
	sort.Slice(sortedItems, func(i, j int) bool {
		iItem, jItem := sortedItems[i], sortedItems[j]
		if counts[iItem] != counts[jItem] {
			return counts[iItem] > counts[jItem] // reverse this check; higher counts should sort lower
		}
		if distances[iItem] != distances[jItem] {
			return distances[iItem] < distances[jItem]
		}
		if generations[iItem] != generations[jItem] {
			return generations[iItem] > generations[jItem] // reverse this check; higher generations should sort lower
		}
		return iItem < jItem // laddr is as good a tiebreaker as anything
	})
	for _, item := range sortedItems {
		if !illegal.Has(item) {
			accept(item)
		}
	}

	for i, list := range lists {
		dlog.Infof(ctx, "... ... ... %d: %v: %v", i, list.Intersection(ret).TakeOne(), maps.SortedKeys(list))
	}

	return ret
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// _NodeFile is a subset of btrfstree.NodeFile.
type _NodeFile interface {
	ParentTree(btrfsprim.ObjID) (btrfsprim.ObjID, bool)
}

func treeDistance(fs _NodeFile, tree, leaf btrfsprim.ObjID) (int, bool) {
	dist := 0
	for {
		if tree == leaf {
			return dist, true
		}

		parentTree, ok := fs.ParentTree(tree)
		if !ok {
			// Failed to look up parent info.
			return 0, false
		}
		if parentTree == 0 {
			// End of the line.
			return 0, false
		}

		tree = parentTree
		dist++
	}
}

func (o *Rebuilder) wantAugment(ctx context.Context, treeID btrfsprim.ObjID, choices containers.Set[btrfsvol.LogicalAddr]) {
	choicesWithDist := make(map[btrfsvol.LogicalAddr]int)
	for choice := range choices {
		if dist, ok := treeDistance(o.uuidMap, treeID, o.graph.Nodes[choice].Owner); ok {
			choicesWithDist[choice] = dist
		}
	}
	if len(choicesWithDist) == 0 {
		dlog.Errorf(ctx, "augment(tree=%v): could not find wanted item", treeID)
		return
	}
	dlog.Infof(ctx, "augment(tree=%v): %v", treeID, maps.SortedKeys(choicesWithDist))
	o.pendingAugments[treeID] = append(o.pendingAugments[treeID], choicesWithDist)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// fsErr implements rebuildCallbacks.
func (o *Rebuilder) fsErr(ctx context.Context, e error) {
	dlog.Errorf(ctx, "filesystem error: %v", e)
}

// want implements rebuildCallbacks.
func (o *Rebuilder) want(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType) {
	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
	}
	if _, err := o.inner.TreeSearch(treeID, func(key btrfsprim.Key, _ uint32) int {
		key.Offset = 0
		return tgt.Cmp(key)
	}); err == nil {
		return
	}

	// OK, we need to insert it

	ctx = dlog.WithField(ctx, "want_key", fmt.Sprintf("tree=%v key={%v %v ?}", treeID, objID, typ))
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.key2leaf.Subrange(
		func(k keyAndTree, _ btrfsvol.LogicalAddr) int { k.Key.Offset = 0; return tgt.Cmp(k.Key) },
		func(_ keyAndTree, v btrfsvol.LogicalAddr) bool {
			wants.InsertFrom(o.leaf2orphans[v])
			return true
		})
	o.wantAugment(ctx, treeID, wants)
}

// wantOff implements rebuildCallbacks.
func (o *Rebuilder) wantOff(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, off uint64) {
	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   off,
	}
	if _, err := o.inner.TreeLookup(treeID, tgt); err == nil {
		return
	}

	// OK, we need to insert it

	ctx = dlog.WithField(ctx, "want_key", fmt.Sprintf("tree=%v key=%v", treeID, tgt))
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.key2leaf.Subrange(
		func(k keyAndTree, _ btrfsvol.LogicalAddr) int { return tgt.Cmp(k.Key) },
		func(_ keyAndTree, v btrfsvol.LogicalAddr) bool {
			wants.InsertFrom(o.leaf2orphans[v])
			return true
		})
	o.wantAugment(ctx, treeID, wants)
}

// wantFunc implements rebuildCallbacks.
func (o *Rebuilder) wantFunc(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, fn func(btrfsitem.Item) bool) {
	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
	}
	items, _ := o.inner.TreeSearchAll(treeID, func(key btrfsprim.Key, _ uint32) int {
		key.Offset = 0
		return tgt.Cmp(key)
	})
	for _, item := range items {
		if fn(item.Body) {
			return
		}
	}

	// OK, we need to insert it

	ctx = dlog.WithField(ctx, "want_key", fmt.Sprintf("tree=%v key=%v +func", treeID, tgt))
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.key2leaf.Subrange(
		func(k keyAndTree, _ btrfsvol.LogicalAddr) int { k.Key.Offset = 0; return tgt.Cmp(k.Key) },
		func(k keyAndTree, v btrfsvol.LogicalAddr) bool {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](o.raw, o.sb, v, btrfstree.NodeExpectations{
				LAddr:      containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: v},
				Generation: containers.Optional[btrfsprim.Generation]{OK: true, Val: o.graph.Nodes[v].Generation},
			})
			if err != nil {
				o.ioErr(ctx, err)
			}
			for _, item := range nodeRef.Data.BodyLeaf {
				if k.Key == item.Key && fn(item.Body) {
					wants.InsertFrom(o.leaf2orphans[v])
				}
			}
			return true
		})
	o.wantAugment(ctx, treeID, wants)
}

// func implements rebuildCallbacks.
//
// interval is [beg, end)
func (o *Rebuilder) wantCSum(ctx context.Context, beg, end btrfsvol.LogicalAddr) {
	for beg < end {
		// check if we already have it
		if run, err := btrfs.LookupCSum(o.inner, o.sb.ChecksumType, beg); err == nil {
			// we already have it
			beg = run.Addr.Add(run.Size())
		} else {
			// we need to insert it
			ctx := dlog.WithField(ctx, "want_key", fmt.Sprintf("csum for laddr=%v", beg))
			rbNode := o.csums.Search(func(item btrfsinspect.SysExtentCSum) int {
				switch {
				case item.Sums.Addr > beg:
					return -1
				case item.Sums.Addr.Add(item.Sums.Size()) <= beg:
					return 1
				default:
					return 0
				}

			})
			if rbNode == nil {
				o.wantAugment(ctx, btrfsprim.CSUM_TREE_OBJECTID, nil) // log an error
				beg += btrfssum.BlockSize
				continue
			}
			run := rbNode.Value.Sums
			key := keyAndTree{
				Key:    rbNode.Value.Key,
				TreeID: btrfsprim.CSUM_TREE_OBJECTID,
			}
			leaf, ok := o.key2leaf.Load(key)
			if !ok {
				// This is a panic because if we found it in `o.csums` then it has
				// to be in some Node, and if we didn't find it from
				// btrfs.LookupCSum(), then that Node must be an orphan.
				panic(fmt.Errorf("should not happen: no orphan contains %v", key.Key))
			}
			o.wantAugment(ctx, key.TreeID, o.leaf2orphans[leaf])

			beg = run.Addr.Add(run.Size())
		}
	}
}

// wantFileExt implements rebuildCallbacks.
func (o *Rebuilder) wantFileExt(ctx context.Context, treeID btrfsprim.ObjID, ino btrfsprim.ObjID, size int64) {
	min := btrfsprim.Key{
		ObjectID: ino,
		ItemType: btrfsitem.EXTENT_DATA_KEY,
		Offset:   0,
	}
	max := btrfsprim.Key{
		ObjectID: ino,
		ItemType: btrfsitem.EXTENT_DATA_KEY,
		Offset:   uint64(size - 1),
	}
	exts, _ := o.inner.TreeSearchAll(treeID, func(key btrfsprim.Key, _ uint32) int {
		switch {
		case min.Cmp(key) < 0:
			return 1
		case max.Cmp(key) > 0:
			return -1
		default:
			return 0
		}
	})

	type gap struct {
		// range is [Beg,End)
		Beg, End int64
	}
	gaps := &containers.RBTree[containers.NativeOrdered[int64], gap]{
		KeyFn: func(gap gap) containers.NativeOrdered[int64] {
			return containers.NativeOrdered[int64]{Val: gap.Beg}
		},
	}
	gaps.Insert(gap{
		Beg: 0,
		End: size,
	})
	for _, ext := range exts {
		switch extBody := ext.Body.(type) {
		case btrfsitem.FileExtent:
			extBeg := int64(ext.Key.Offset)
			extSize, err := extBody.Size()
			if err != nil {
				o.fsErr(ctx, fmt.Errorf("FileExtent: tree=%v key=%v: %w", treeID, ext.Key, err))
				continue
			}
			extEnd := extBeg + extSize
			overlappingGaps := gaps.SearchRange(func(gap gap) int {
				switch {
				case gap.End <= extBeg:
					return 1
				case extEnd <= gap.Beg:
					return -1
				default:
					return 0
				}
			})
			if len(overlappingGaps) == 0 {
				continue
			}
			beg := overlappingGaps[0].Beg
			end := overlappingGaps[len(overlappingGaps)-1].End
			for _, gap := range overlappingGaps {
				gaps.Delete(containers.NativeOrdered[int64]{Val: gap.Beg})
			}
			if beg < extBeg {
				gaps.Insert(gap{
					Beg: beg,
					End: extBeg,
				})
			}
			if end > extEnd {
				gaps.Insert(gap{
					Beg: extEnd,
					End: end,
				})
			}
		case btrfsitem.Error:
			o.fsErr(ctx, fmt.Errorf("error decoding item: tree=%v key=%v: %w", treeID, ext.Key, extBody.Err))
		default:
			// This is a panic because the item decoder should not emit EXTENT_DATA
			// items as anything but btrfsitem.FileExtent or btrfsitem.Error without
			// this code also being updated.
			panic(fmt.Errorf("should not happen: EXTENT_DATA item has unexpected type: %T", extBody))
		}
	}
	_ = gaps.Walk(func(rbNode *containers.RBNode[gap]) error {
		gap := rbNode.Value
		min := btrfsprim.Key{
			ObjectID: ino,
			ItemType: btrfsitem.EXTENT_DATA_KEY,
			Offset:   0,
		}
		max := btrfsprim.Key{
			ObjectID: ino,
			ItemType: btrfsitem.EXTENT_DATA_KEY,
			Offset:   uint64(gap.End - 1),
		}
		ctx := dlog.WithField(ctx, "want_key", fmt.Sprintf("file extent for tree=%v inode=%v bytes [%v, %v)", treeID, ino, gap.Beg, gap.End))
		wants := make(containers.Set[btrfsvol.LogicalAddr])
		o.key2leaf.Subrange(
			func(k keyAndTree, _ btrfsvol.LogicalAddr) int {
				switch {
				case min.Cmp(k.Key) < 0:
					return 1
				case max.Cmp(k.Key) > 0:
					return -1
				default:
					return 0
				}
			},
			func(k keyAndTree, v btrfsvol.LogicalAddr) bool {
				nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](o.raw, o.sb, v, btrfstree.NodeExpectations{
					LAddr:      containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: v},
					Generation: containers.Optional[btrfsprim.Generation]{OK: true, Val: o.graph.Nodes[v].Generation},
				})
				if err != nil {
					o.ioErr(ctx, fmt.Errorf("error reading previously read node@%v: %w", v, err))
				}
				for _, item := range nodeRef.Data.BodyLeaf {
					if k.Key != item.Key {
						continue
					}
					switch itemBody := item.Body.(type) {
					case btrfsitem.FileExtent:
						itemBeg := int64(item.Key.Offset)
						itemSize, err := itemBody.Size()
						if err != nil {
							o.fsErr(ctx, fmt.Errorf("FileExtent: tree=%v key=%v: %w", treeID, item.Key, err))
							continue
						}
						itemEnd := itemBeg + itemSize
						// We're being and "wanting" any extent that has any overlap with the
						// gap.  But maybe instead we sould only want extents that are
						// *entirely* within the gap.  I'll have to run it on real filesystems
						// to see what works better.
						//
						// TODO(lukeshu): Re-evaluate whether being greedy here is the right
						// thing.
						if itemEnd > gap.Beg && itemBeg < gap.End {
							wants.InsertFrom(o.leaf2orphans[v])
						}
					case btrfsitem.Error:
						o.fsErr(ctx, fmt.Errorf("error decoding item: tree=%v key=%v: %w", treeID, item.Key, itemBody.Err))
					default:
						// This is a panic because the item decoder should not emit EXTENT_DATA
						// items as anything but btrfsitem.FileExtent or btrfsitem.Error without
						// this code also being updated.
						panic(fmt.Errorf("should not happen: EXTENT_DATA item has unexpected type: %T", itemBody))
					}
				}
				return true
			})
		o.wantAugment(ctx, treeID, wants)
		return nil
	})
}
