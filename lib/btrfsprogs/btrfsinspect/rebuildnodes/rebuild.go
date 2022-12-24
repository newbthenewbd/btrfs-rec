// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildmappings"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/btrees"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/keyio"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type keyAndTree struct {
	btrfsprim.Key
	TreeID btrfsprim.ObjID
}

func (a keyAndTree) Cmp(b keyAndTree) int {
	if d := a.Key.Cmp(b.Key); d != 0 {
		return d
	}
	return containers.NativeCmp(a.TreeID, b.TreeID)
}

type rebuilder struct {
	sb      btrfstree.Superblock
	rebuilt *btrees.RebuiltTrees

	graph graph.Graph
	csums containers.RBTree[containers.NativeOrdered[btrfsvol.LogicalAddr], btrfsinspect.SysExtentCSum]
	keyIO *keyio.Handle

	curKey          keyAndTree
	queue           []keyAndTree
	pendingAugments map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int
}

func RebuildNodes(ctx context.Context, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) (map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], error) {
	nodeGraph, err := ScanDevices(ctx, fs, nodeScanResults) // ScanDevices does its own logging
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

	dlog.Info(ctx, "Rebuilding node tree...")
	keyIO := keyio.NewHandle(fs, *sb, nodeGraph)
	o := &rebuilder{
		sb: *sb,

		graph: nodeGraph,
		csums: *csums,
		keyIO: keyIO,
	}
	o.rebuilt = btrees.NewRebuiltTrees(*sb, nodeGraph, keyIO,
		o.cbAddedItem, o.cbLookupRoot, o.cbLookupUUID)
	if err := o.rebuild(ctx); err != nil {
		return nil, err
	}

	return o.rebuilt.ListRoots(), nil
}

func (o *rebuilder) ioErr(ctx context.Context, err error) {
	err = fmt.Errorf("should not happen: i/o error: %w", err)
	dlog.Error(ctx, err)
	panic(err)
}

type rebuildStats struct {
	PassNum int
	Task    string
	N, D    int
}

func (s rebuildStats) String() string {
	pct := 100
	if s.D > 0 {
		pct = int(100 * float64(s.N) / float64(s.D))
	}
	return fmt.Sprintf("... pass %d: %s %v%% (%v/%v)",
		s.PassNum, s.Task, pct, s.N, s.D)
}

func (o *rebuilder) rebuild(ctx context.Context) error {
	passNum := 0
	dlog.Infof(ctx, "... pass %d: scanning for implied items", passNum)
	// Seed the queue
	o.pendingAugments = make(map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int)
	o.rebuilt.AddTree(ctx, btrfsprim.ROOT_TREE_OBJECTID)
	o.rebuilt.AddTree(ctx, btrfsprim.CHUNK_TREE_OBJECTID)
	//o.rebuilt.AddTree(ctx, btrfsprim.TREE_LOG_OBJECTID) // TODO(lukeshu): Special LOG_TREE handling
	o.rebuilt.AddTree(ctx, btrfsprim.BLOCK_GROUP_TREE_OBJECTID)
	for {
		// Handle items in the queue
		queue := o.queue
		o.queue = nil
		progressWriter := textui.NewProgress[rebuildStats](ctx, dlog.LogLevelInfo, 1*time.Second)
		queueProgress := func(done int) {
			progressWriter.Set(rebuildStats{
				PassNum: passNum,
				Task:    "processing item queue",
				N:       done,
				D:       len(queue),
			})
		}
		for i, key := range queue {
			queueProgress(i)
			o.curKey = key
			itemBody, ok := o.rebuilt.Load(ctx, key.TreeID, key.Key)
			if !ok {
				o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v", key))
			}
			handleItem(o, ctx, key.TreeID, btrfstree.Item{
				Key:  key.Key,
				Body: itemBody,
			})
			if key.ItemType == btrfsitem.ROOT_ITEM_KEY {
				o.rebuilt.AddTree(ctx, key.ObjectID)
			}
		}
		queueProgress(len(queue))
		progressWriter.Done()

		// Check if we can bail
		if len(o.queue) == 0 && len(o.pendingAugments) == 0 {
			break
		}

		// Apply augments that were requested while handling items from the queue
		resolvedAugments := make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], len(o.pendingAugments))
		numAugments := 0
		for _, treeID := range maps.SortedKeys(o.pendingAugments) {
			dlog.Infof(ctx, "... ... augments for tree %v:", treeID)
			resolvedAugments[treeID] = o.resolveTreeAugments(ctx, o.pendingAugments[treeID])
			numAugments += len(resolvedAugments[treeID])
		}
		o.pendingAugments = make(map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int)
		progressWriter = textui.NewProgress[rebuildStats](ctx, dlog.LogLevelInfo, 1*time.Second)
		numAugmented := 0
		augmentProgress := func() {
			progressWriter.Set(rebuildStats{
				PassNum: passNum,
				Task:    "applying augments",
				N:       numAugmented,
				D:       numAugments,
			})
		}
		for _, treeID := range maps.SortedKeys(resolvedAugments) {
			for _, nodeAddr := range maps.SortedKeys(resolvedAugments[treeID]) {
				augmentProgress()
				o.rebuilt.AddRoot(ctx, treeID, nodeAddr)
				numAugmented++
			}
		}
		augmentProgress()
		progressWriter.Done()

		passNum++
		dlog.Infof(ctx, "... pass %d: scanning for implied items", passNum)
	}
	return nil
}

func (o *rebuilder) cbAddedItem(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
	o.queue = append(o.queue, keyAndTree{
		TreeID: tree,
		Key:    key,
	})
}

func (o *rebuilder) cbLookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
	key, ok := o._want(ctx, btrfsprim.ROOT_TREE_OBJECTID, tree, btrfsitem.ROOT_ITEM_KEY)
	if !ok {
		o.queue = append(o.queue, o.curKey)
		return 0, btrfsitem.Root{}, false
	}
	itemBody, ok := o.rebuilt.Load(ctx, btrfsprim.ROOT_TREE_OBJECTID, key)
	if !ok {
		o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v", key))
	}
	switch itemBody := itemBody.(type) {
	case btrfsitem.Root:
		return btrfsprim.Generation(key.Offset), itemBody, true
	case btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: tree=%v key=%v: %w", btrfsprim.ROOT_TREE_OBJECTID, key, itemBody.Err))
		return 0, btrfsitem.Root{}, false
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

func (o *rebuilder) cbLookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
	key := btrfsitem.UUIDToKey(uuid)
	if ok := o._wantOff(ctx, btrfsprim.UUID_TREE_OBJECTID, key.ObjectID, key.ItemType, key.Offset); !ok {
		o.queue = append(o.queue, o.curKey)
		return 0, false
	}
	itemBody, ok := o.rebuilt.Load(ctx, btrfsprim.UUID_TREE_OBJECTID, key)
	if !ok {
		o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v", key))
	}
	switch itemBody := itemBody.(type) {
	case btrfsitem.UUIDMap:
		return itemBody.ObjID, true
	case btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: tree=%v key=%v: %w", btrfsprim.UUID_TREE_OBJECTID, key, itemBody.Err))
		return 0, false
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}

func (o *rebuilder) resolveTreeAugments(ctx context.Context, listsWithDistances []map[btrfsvol.LogicalAddr]int) containers.Set[btrfsvol.LogicalAddr] {
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
		chose := list.Intersection(ret)
		if len(chose) == 0 {
			dlog.Infof(ctx, "... ... ... lists[%d]: chose (none) from %v", i, maps.SortedKeys(list))
		} else {
			dlog.Infof(ctx, "... ... ... lists[%d]: chose %v from %v", i, chose.TakeOne(), maps.SortedKeys(list))
		}
	}

	return ret
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (o *rebuilder) wantAugment(ctx context.Context, treeID btrfsprim.ObjID, choices containers.Set[btrfsvol.LogicalAddr]) {
	if len(choices) == 0 {
		dlog.Errorf(ctx, "augment(tree=%v): could not find wanted item", treeID)
		return
	}
	choicesWithDist := make(map[btrfsvol.LogicalAddr]int, len(choices))
	for choice := range choices {
		dist, ok := o.rebuilt.COWDistance(ctx, treeID, o.graph.Nodes[choice].Owner)
		if !ok {
			panic(fmt.Errorf("should not happen: .wantAugment called for tree=%v with invalid choice=%v", treeID, choice))
		}
		choicesWithDist[choice] = dist
	}
	dlog.Infof(ctx, "augment(tree=%v): %v", treeID, maps.SortedKeys(choicesWithDist))
	o.pendingAugments[treeID] = append(o.pendingAugments[treeID], choicesWithDist)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// fsErr implements rebuildCallbacks.
func (o *rebuilder) fsErr(ctx context.Context, e error) {
	dlog.Errorf(ctx, "filesystem error: %v", e)
}

// want implements rebuildCallbacks.
func (o *rebuilder) want(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType) {
	o._want(ctx, treeID, objID, typ)
}
func (o *rebuilder) _want(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType) (key btrfsprim.Key, ok bool) {
	if !o.rebuilt.AddTree(ctx, treeID) {
		o.queue = append(o.queue, o.curKey)
		return btrfsprim.Key{}, false
	}

	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
	}
	if key, ok := o.rebuilt.Search(ctx, treeID, func(key btrfsprim.Key) int {
		key.Offset = 0
		return tgt.Cmp(key)
	}); ok {
		return key, true
	}

	// OK, we need to insert it

	ctx = dlog.WithField(ctx, "want_key", fmt.Sprintf("tree=%v key={%v %v ?}", treeID, objID, typ))
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.rebuilt.Keys(treeID).Subrange(
		func(k btrfsprim.Key, _ keyio.ItemPtr) int { k.Offset = 0; return tgt.Cmp(k) },
		func(_ btrfsprim.Key, v keyio.ItemPtr) bool {
			wants.InsertFrom(o.rebuilt.LeafToRoots(ctx, treeID, v.Node))
			return true
		})
	o.wantAugment(ctx, treeID, wants)
	return btrfsprim.Key{}, false
}

// wantOff implements rebuildCallbacks.
func (o *rebuilder) wantOff(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, off uint64) {
	o._wantOff(ctx, treeID, objID, typ, off)
}
func (o *rebuilder) _wantOff(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, off uint64) (ok bool) {
	if !o.rebuilt.AddTree(ctx, treeID) {
		o.queue = append(o.queue, o.curKey)
		return false
	}

	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   off,
	}
	if _, ok := o.rebuilt.Load(ctx, treeID, tgt); ok {
		return true
	}

	// OK, we need to insert it

	ctx = dlog.WithField(ctx, "want_key", fmt.Sprintf("tree=%v key=%v", treeID, tgt))
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.rebuilt.Keys(treeID).Subrange(
		func(k btrfsprim.Key, _ keyio.ItemPtr) int { return tgt.Cmp(k) },
		func(_ btrfsprim.Key, v keyio.ItemPtr) bool {
			wants.InsertFrom(o.rebuilt.LeafToRoots(ctx, treeID, v.Node))
			return true
		})
	o.wantAugment(ctx, treeID, wants)
	return false
}

// wantFunc implements rebuildCallbacks.
func (o *rebuilder) wantFunc(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, fn func(btrfsitem.Item) bool) {
	if !o.rebuilt.AddTree(ctx, treeID) {
		o.queue = append(o.queue, o.curKey)
		return
	}

	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
	}
	keys := o.rebuilt.SearchAll(ctx, treeID, func(key btrfsprim.Key) int {
		key.Offset = 0
		return tgt.Cmp(key)
	})
	for _, itemKey := range keys {
		itemBody, ok := o.rebuilt.Load(ctx, treeID, itemKey)
		if !ok {
			o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v", itemKey))
		}
		if fn(itemBody) {
			return
		}
	}

	// OK, we need to insert it

	ctx = dlog.WithField(ctx, "want_key", fmt.Sprintf("tree=%v key=%v +func", treeID, tgt))
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.rebuilt.Keys(treeID).Subrange(
		func(k btrfsprim.Key, _ keyio.ItemPtr) int { k.Offset = 0; return tgt.Cmp(k) },
		func(k btrfsprim.Key, v keyio.ItemPtr) bool {
			itemBody, ok := o.keyIO.ReadItem(v)
			if !ok {
				o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v at %v", k, v))
			}
			if fn(itemBody) {
				wants.InsertFrom(o.rebuilt.LeafToRoots(ctx, treeID, v.Node))
			}
			return true
		})
	o.wantAugment(ctx, treeID, wants)
}

// func implements rebuildCallbacks.
//
// interval is [beg, end)
func (o *rebuilder) wantCSum(ctx context.Context, beg, end btrfsvol.LogicalAddr) {
	treeID := btrfsprim.CSUM_TREE_OBJECTID
	if !o.rebuilt.AddTree(ctx, treeID) {
		o.queue = append(o.queue, o.curKey)
		return
	}

	last := beg
	for beg < end {
		// Figure out which key we want.

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
			beg += btrfssum.BlockSize
			continue
		} else if last < beg {
			dlog.Errorf(ctx, "augment(tree=%v): could not find csum items for %v-%v", treeID, last, beg)
		}
		run := rbNode.Value.Sums
		key := rbNode.Value.Key

		// Check if we already have it.

		// .Search is more efficient than .Load, because it doesn't load the body (and we don't need the body).
		if _, ok := o.rebuilt.Search(ctx, treeID, key.Cmp); !ok {
			// We need to insert it.
			itemPtr, ok := o.rebuilt.Keys(treeID).Load(key)
			if !ok {
				// This is a panic because if we found it in `o.csums` then it has
				// to be in some Node, and if we didn't find it from
				// btrfs.LookupCSum(), then that Node must be an orphan.
				panic(fmt.Errorf("should not happen: no orphan contains %v", key))
			}
			o.wantAugment(ctx, treeID, o.rebuilt.LeafToRoots(ctx, treeID, itemPtr.Node))
		}

		beg = run.Addr.Add(run.Size())
		last = beg
	}
	if last < beg {
		dlog.Errorf(ctx, "augment(tree=%v): could not find csum items for %v-%v", treeID, last, beg)
	}
}

// wantFileExt implements rebuildCallbacks.
func (o *rebuilder) wantFileExt(ctx context.Context, treeID btrfsprim.ObjID, ino btrfsprim.ObjID, size int64) {
	if !o.rebuilt.AddTree(ctx, treeID) {
		o.queue = append(o.queue, o.curKey)
		return
	}

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
	extKeys := o.rebuilt.SearchAll(ctx, treeID, func(key btrfsprim.Key) int {
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
	for _, extKey := range extKeys {
		extBody, ok := o.rebuilt.Load(ctx, treeID, extKey)
		if !ok {
			o.ioErr(ctx, fmt.Errorf("could not look up already-inserted item: tree=%v key=%v",
				treeID, extKey))
		}
		switch extBody := extBody.(type) {
		case btrfsitem.FileExtent:
			extBeg := int64(extKey.Offset)
			extSize, err := extBody.Size()
			if err != nil {
				o.fsErr(ctx, fmt.Errorf("FileExtent: tree=%v key=%v: %w", treeID, extKey, err))
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
			o.fsErr(ctx, fmt.Errorf("error decoding item: tree=%v key=%v: %w", treeID, extKey, extBody.Err))
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
		o.rebuilt.Keys(treeID).Subrange(
			func(k btrfsprim.Key, _ keyio.ItemPtr) int {
				switch {
				case min.Cmp(k) < 0:
					return 1
				case max.Cmp(k) > 0:
					return -1
				default:
					return 0
				}
			},
			func(k btrfsprim.Key, v keyio.ItemPtr) bool {
				itemBody, ok := o.keyIO.ReadItem(v)
				if !ok {
					o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v", v))
				}
				switch itemBody := itemBody.(type) {
				case btrfsitem.FileExtent:
					itemBeg := int64(k.Offset)
					itemSize, err := itemBody.Size()
					if err != nil {
						o.fsErr(ctx, fmt.Errorf("FileExtent: tree=%v key=%v: %w", treeID, k, err))
						break
					}
					itemEnd := itemBeg + itemSize
					// We're being greedy and "wanting" any extent that has any overlap with
					// the gap.  But maybe instead we sould only want extents that are
					// *entirely* within the gap.  I'll have to run it on real filesystems
					// to see what works better.
					//
					// TODO(lukeshu): Re-evaluate whether being greedy here is the right
					// thing.
					if itemEnd > gap.Beg && itemBeg < gap.End {
						wants.InsertFrom(o.rebuilt.LeafToRoots(ctx, treeID, v.Node))
					}
				case btrfsitem.Error:
					o.fsErr(ctx, fmt.Errorf("error decoding item: tree=%v key=%v: %w", treeID, k, itemBody.Err))
				default:
					// This is a panic because the item decoder should not emit EXTENT_DATA
					// items as anything but btrfsitem.FileExtent or btrfsitem.Error without
					// this code also being updated.
					panic(fmt.Errorf("should not happen: EXTENT_DATA item has unexpected type: %T", itemBody))
				}
				return true
			})
		o.wantAugment(ctx, treeID, wants)
		return nil
	})
}
