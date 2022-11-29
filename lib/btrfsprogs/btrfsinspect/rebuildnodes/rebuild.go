// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"

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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type Rebuilder struct {
	raw   *btrfs.FS
	inner interface {
		btrfstree.TreeOperator
		Augment(treeID btrfsprim.ObjID, nodeAddr btrfsvol.LogicalAddr) ([]btrfsprim.Key, error)
	}
	sb btrfstree.Superblock

	graph        graph.Graph
	csums        containers.RBTree[containers.NativeOrdered[btrfsvol.LogicalAddr], btrfsinspect.SysExtentCSum]
	orphans      containers.Set[btrfsvol.LogicalAddr]
	leaf2orphans map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]
	key2leaf     containers.SortedMap[keyAndTree, btrfsvol.LogicalAddr]

	augments map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]
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
	orphans, leaf2orphans, key2leaf, err := indexOrphans(fs, *sb, *scanData.nodeGraph)
	if err != nil {
		return nil, err
	}

	dlog.Info(ctx, "Rebuilding node tree...")
	o := &Rebuilder{
		raw:   fs,
		inner: btrfsutil.NewBrokenTrees(ctx, fs),
		sb:    *sb,

		graph:        *scanData.nodeGraph,
		csums:        *csums,
		orphans:      orphans,
		leaf2orphans: leaf2orphans,
		key2leaf:     *key2leaf,

		augments: make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]),
	}
	if err := o.rebuild(ctx); err != nil {
		return nil, err
	}

	return o.augments, nil
}

func (o *Rebuilder) rebuild(ctx context.Context) error {
	// TODO
	//btrfsutil.WalkAllTrees(ctx, o.inner)
	handleItem(o, ctx, 0, btrfstree.Item{})
	return nil
}

func (o *Rebuilder) wantAugment(ctx context.Context, treeID btrfsprim.ObjID, choices containers.Set[btrfsvol.LogicalAddr]) {
	// TODO
}

// err implements rebuildCallbacks.
func (o *Rebuilder) err(ctx context.Context, e error) {
	dlog.Errorf(ctx, "rebuild error: %v", e)
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

	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.key2leaf.Subrange(
		func(k keyAndTree, _ btrfsvol.LogicalAddr) int { k.Key.Offset = 0; return tgt.Cmp(k.Key) },
		func(k keyAndTree, v btrfsvol.LogicalAddr) bool {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](o.raw, o.sb, v, btrfstree.NodeExpectations{
				LAddr:      containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: v},
				Generation: containers.Optional[btrfsprim.Generation]{OK: true, Val: o.graph.Nodes[v].Generation},
			})
			if err != nil {
				o.err(ctx, err)
				return true
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
				o.err(ctx, fmt.Errorf("could not find csum for laddr=%v", beg))
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
				panic(fmt.Errorf("no orphan contains %v", key.Key))
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
	exts, err := o.inner.TreeSearchAll(treeID, func(key btrfsprim.Key, _ uint32) int {
		switch {
		case min.Cmp(key) < 0:
			return 1
		case max.Cmp(key) > 0:
			return -1
		default:
			return 0
		}
	})
	if err != nil {
		o.err(ctx, err)
		return
	}

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
		extBody, ok := ext.Body.(btrfsitem.FileExtent)
		if !ok {
			o.err(ctx, fmt.Errorf("EXTENT_DATA is %T", ext.Body))
			continue
		}
		extBeg := int64(ext.Key.Offset)
		extSize, err := extBody.Size()
		if err != nil {
			o.err(ctx, err)
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
	}
	if err := gaps.Walk(func(rbNode *containers.RBNode[gap]) error {
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
					o.err(ctx, err)
					return true
				}
				for _, item := range nodeRef.Data.BodyLeaf {
					if k.Key == item.Key {
						itemBeg := int64(item.Key.Offset)
						itemBody, ok := item.Body.(btrfsitem.FileExtent)
						if !ok {
							o.err(ctx, fmt.Errorf("EXTENT_DATA is %T", item.Body))
							continue
						}
						itemSize, err := itemBody.Size()
						if err != nil {
							o.err(ctx, err)
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
					}
				}
				return true
			})
		o.wantAugment(ctx, treeID, wants)
		return nil
	}); err != nil {
		o.err(ctx, err)
	}
}
