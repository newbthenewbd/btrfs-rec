// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"bytes"
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/keyio"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

// fsErr implements rebuildCallbacks.
func (o *rebuilder) fsErr(ctx context.Context, e error) {
	dlog.Errorf(ctx, "filesystem error: %v", e)
}

// want implements rebuildCallbacks.
func (o *rebuilder) want(ctx context.Context, reason string, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
	wantKey := fmt.Sprintf("tree=%v key={%v %v ?}", treeID, objID, typ)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", wantKey)
	o._want(ctx, treeID, wantKey, objID, typ)
}

func (o *rebuilder) _want(ctx context.Context, treeID btrfsprim.ObjID, wantKey string, objID btrfsprim.ObjID, typ btrfsprim.ItemType) (key btrfsprim.Key, ok bool) {
	if o.rebuilt.Tree(ctx, treeID) == nil {
		o.enqueueRetry()
		return btrfsprim.Key{}, false
	}

	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
	}
	if key, _, ok := o.rebuilt.Tree(ctx, treeID).Items(ctx).Search(func(key btrfsprim.Key, _ keyio.ItemPtr) int {
		key.Offset = 0
		return tgt.Compare(key)
	}); ok {
		return key, true
	}

	// OK, we need to insert it

	if o.hasAugment(treeID, wantKey) {
		return btrfsprim.Key{}, false
	}
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.rebuilt.Tree(ctx, treeID).PotentialItems(ctx).Subrange(
		func(k btrfsprim.Key, _ keyio.ItemPtr) int { k.Offset = 0; return tgt.Compare(k) },
		func(_ btrfsprim.Key, v keyio.ItemPtr) bool {
			wants.InsertFrom(o.rebuilt.Tree(ctx, treeID).LeafToRoots(ctx, v.Node))
			return true
		})
	o.wantAugment(ctx, treeID, wantKey, wants)
	return btrfsprim.Key{}, false
}

// wantOff implements rebuildCallbacks.
func (o *rebuilder) wantOff(ctx context.Context, reason string, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, off uint64) {
	key := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   off,
	}
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
	wantKey := keyAndTree{TreeID: treeID, Key: key}.String()
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", wantKey)
	o._wantOff(ctx, treeID, wantKey, key)
}

func (o *rebuilder) _wantOff(ctx context.Context, treeID btrfsprim.ObjID, wantKey string, tgt btrfsprim.Key) (ok bool) {
	if o.rebuilt.Tree(ctx, treeID) == nil {
		o.enqueueRetry()
		return false
	}

	// check if we already have it

	if _, ok := o.rebuilt.Tree(ctx, treeID).Items(ctx).Load(tgt); ok {
		return true
	}

	// OK, we need to insert it

	if o.hasAugment(treeID, wantKey) {
		return false
	}
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.rebuilt.Tree(ctx, treeID).PotentialItems(ctx).Subrange(
		func(k btrfsprim.Key, _ keyio.ItemPtr) int { return tgt.Compare(k) },
		func(_ btrfsprim.Key, v keyio.ItemPtr) bool {
			wants.InsertFrom(o.rebuilt.Tree(ctx, treeID).LeafToRoots(ctx, v.Node))
			return true
		})
	o.wantAugment(ctx, treeID, wantKey, wants)
	return false
}

func (o *rebuilder) _wantFunc(ctx context.Context, treeID btrfsprim.ObjID, wantKey string, objID btrfsprim.ObjID, typ btrfsprim.ItemType, fn func(keyio.ItemPtr) bool) {
	if o.rebuilt.Tree(ctx, treeID) == nil {
		o.enqueueRetry()
		return
	}

	// check if we already have it

	tgt := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
	}
	found := false
	o.rebuilt.Tree(ctx, treeID).Items(ctx).Subrange(
		func(key btrfsprim.Key, _ keyio.ItemPtr) int {
			key.Offset = 0
			return tgt.Compare(key)
		},
		func(_ btrfsprim.Key, itemPtr keyio.ItemPtr) bool {
			if fn(itemPtr) {
				found = true
			}
			return !found
		})
	if found {
		return
	}

	// OK, we need to insert it

	if o.hasAugment(treeID, wantKey) {
		return
	}
	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.rebuilt.Tree(ctx, treeID).PotentialItems(ctx).Subrange(
		func(k btrfsprim.Key, _ keyio.ItemPtr) int { k.Offset = 0; return tgt.Compare(k) },
		func(k btrfsprim.Key, v keyio.ItemPtr) bool {
			if fn(v) {
				wants.InsertFrom(o.rebuilt.Tree(ctx, treeID).LeafToRoots(ctx, v.Node))
			}
			return true
		})
	o.wantAugment(ctx, treeID, wantKey, wants)
}

// wantDirIndex implements rebuildCallbacks.
func (o *rebuilder) wantDirIndex(ctx context.Context, reason string, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, name []byte) {
	typ := btrfsitem.DIR_INDEX_KEY
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
	wantKey := fmt.Sprintf("tree=%v key={%v %v ?} name=%q", treeID, objID, typ, name)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", wantKey)
	o._wantFunc(ctx, treeID, wantKey, objID, typ, func(ptr keyio.ItemPtr) bool {
		itemName, ok := o.keyIO.Names[ptr]
		return ok && bytes.Equal(itemName, name)
	})
}

func (o *rebuilder) _walkRange(
	ctx context.Context,
	items *containers.SortedMap[btrfsprim.Key, keyio.ItemPtr],
	treeID, objID btrfsprim.ObjID, typ btrfsprim.ItemType,
	beg, end uint64,
	fn func(key btrfsprim.Key, ptr keyio.ItemPtr, beg, end uint64),
) {
	min := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   0, // *NOT* `beg`
	}
	max := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   end - 1,
	}
	items.Subrange(
		func(runKey btrfsprim.Key, _ keyio.ItemPtr) int {
			switch {
			case min.Compare(runKey) < 0:
				return 1
			case max.Compare(runKey) > 0:
				return -1
			default:
				return 0
			}
		},
		func(runKey btrfsprim.Key, runPtr keyio.ItemPtr) bool {
			runSizeAndErr, ok := o.keyIO.Sizes[runPtr]
			if !ok {
				panic(fmt.Errorf("should not happen: %v (%v) did not have a size recorded",
					runPtr, keyAndTree{TreeID: treeID, Key: runKey}))
			}
			if runSizeAndErr.Err != nil {
				o.fsErr(ctx, fmt.Errorf("get size: %v (%v): %w",
					runPtr, keyAndTree{TreeID: treeID, Key: runKey},
					runSizeAndErr.Err))
				return true
			}
			runSize := runSizeAndErr.Size
			if runSize == 0 {
				return true
			}
			runBeg := runKey.Offset
			runEnd := runBeg + runSize
			if runEnd <= beg {
				return true
			}

			fn(runKey, runPtr, runBeg, runEnd)
			return true
		})
}

type gap struct {
	// range is [Beg,End)
	Beg, End uint64
}

// Compare implements containers.Ordered.
func (a gap) Compare(b gap) int {
	return containers.NativeCompare(a.Beg, b.Beg)
}

func (o *rebuilder) _wantRange(
	ctx context.Context,
	treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType,
	beg, end uint64,
) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key",
		fmt.Sprintf("tree=%v key={%v %v ?}", treeID, objID, typ))

	if o.rebuilt.Tree(ctx, treeID) == nil {
		o.enqueueRetry()
		return
	}

	// Step 1: Build a listing of the gaps.
	//
	// Start with a gap of the whole range, then subtract each run
	// from it.
	gaps := new(containers.RBTree[gap])
	gaps.Insert(gap{
		Beg: beg,
		End: end,
	})
	o._walkRange(
		ctx,
		o.rebuilt.Tree(ctx, treeID).Items(ctx),
		treeID, objID, typ, beg, end,
		func(runKey btrfsprim.Key, _ keyio.ItemPtr, runBeg, runEnd uint64) {
			var overlappingGaps []*containers.RBNode[gap]
			gaps.Subrange(
				func(gap gap) int {
					switch {
					case gap.End <= runBeg:
						return 1
					case runEnd <= gap.Beg:
						return -1
					default:
						return 0
					}
				},
				func(node *containers.RBNode[gap]) bool {
					overlappingGaps = append(overlappingGaps, node)
					return true
				})
			if len(overlappingGaps) == 0 {
				return
			}
			gapsBeg := overlappingGaps[0].Value.Beg
			gapsEnd := overlappingGaps[len(overlappingGaps)-1].Value.End
			for _, gap := range overlappingGaps {
				gaps.Delete(gap)
			}
			if gapsBeg < runBeg {
				gaps.Insert(gap{
					Beg: gapsBeg,
					End: runBeg,
				})
			}
			if gapsEnd > runEnd {
				gaps.Insert(gap{
					Beg: runEnd,
					End: gapsEnd,
				})
			}
		})

	// Step 2: Fill each gap.
	if gaps.Len() == 0 {
		return
	}
	potentialItems := o.rebuilt.Tree(ctx, treeID).PotentialItems(ctx)
	gaps.Range(func(rbNode *containers.RBNode[gap]) bool {
		gap := rbNode.Value
		last := gap.Beg
		o._walkRange(
			ctx,
			potentialItems,
			treeID, objID, typ, gap.Beg, gap.End,
			func(k btrfsprim.Key, v keyio.ItemPtr, runBeg, runEnd uint64) {
				// TODO: This is dumb and greedy.
				if last < runBeg {
					// log an error
					wantKey := fmt.Sprintf("tree=%v key={%v %v %v-%v}", treeID, objID, typ, last, runBeg)
					if !o.hasAugment(treeID, wantKey) {
						wantCtx := dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", wantKey)
						o.wantAugment(wantCtx, treeID, wantKey, nil)
					}
				}
				wantKey := fmt.Sprintf("tree=%v key={%v %v %v-%v}", treeID, objID, typ, gap.Beg, gap.End)
				if !o.hasAugment(treeID, wantKey) {
					wantCtx := dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", wantKey)
					o.wantAugment(wantCtx, treeID, wantKey, o.rebuilt.Tree(wantCtx, treeID).LeafToRoots(wantCtx, v.Node))
				}
				last = runEnd
			})
		if last < gap.End {
			// log an error
			wantKey := fmt.Sprintf("tree=%v key={%v, %v, %v-%v}", treeID, objID, typ, last, gap.End)
			if !o.hasAugment(treeID, wantKey) {
				wantCtx := dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", wantKey)
				o.wantAugment(wantCtx, treeID, wantKey, nil)
			}
		}
		return true
	})
}

// func implements rebuildCallbacks.
//
// interval is [beg, end)
func (o *rebuilder) wantCSum(ctx context.Context, reason string, inodeTree, inode btrfsprim.ObjID, beg, end btrfsvol.LogicalAddr) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)

	inodeKey := keyAndTree{
		TreeID: inodeTree,
		Key: btrfsprim.Key{
			ObjectID: inode,
			ItemType: btrfsitem.INODE_ITEM_KEY,
			Offset:   0,
		},
	}
	inodeCtx := dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", inodeKey.String())
	if !o._wantOff(inodeCtx, inodeKey.TreeID, inodeKey.String(), inodeKey.Key) {
		o.enqueueRetry()
		return
	}
	inodePtr, ok := o.rebuilt.Tree(inodeCtx, inodeKey.TreeID).Items(inodeCtx).Load(inodeKey.Key)
	if !ok {
		panic(fmt.Errorf("should not happen: could not load key: %v", inodeKey))
	}
	inodeFlags, ok := o.keyIO.Flags[inodePtr]
	if !ok {
		panic(fmt.Errorf("should not happen: INODE_ITEM did not have flags recorded"))
	}
	if inodeFlags.Err != nil {
		o.fsErr(inodeCtx, inodeFlags.Err)
		return
	}

	if inodeFlags.NoDataSum {
		return
	}

	beg = roundDown(beg, btrfssum.BlockSize)
	end = roundUp(end, btrfssum.BlockSize)
	const treeID = btrfsprim.CSUM_TREE_OBJECTID
	o._wantRange(ctx, treeID, btrfsprim.EXTENT_CSUM_OBJECTID, btrfsprim.EXTENT_CSUM_KEY,
		uint64(beg), uint64(end))
}

// wantFileExt implements rebuildCallbacks.
func (o *rebuilder) wantFileExt(ctx context.Context, reason string, treeID btrfsprim.ObjID, ino btrfsprim.ObjID, size int64) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
	o._wantRange(ctx, treeID, ino, btrfsprim.EXTENT_DATA_KEY,
		0, uint64(size))
}
