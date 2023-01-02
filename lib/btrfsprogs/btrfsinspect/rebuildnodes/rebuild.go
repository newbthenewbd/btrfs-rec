// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
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

func (o keyAndTree) String() string {
	return fmt.Sprintf("tree=%v key=%v", o.TreeID, o.Key)
}

type rebuilder struct {
	sb      btrfstree.Superblock
	rebuilt *btrees.RebuiltTrees

	graph graph.Graph
	keyIO *keyio.Handle

	curKey       keyAndTree
	treeQueue    []btrfsprim.ObjID
	itemQueue    []keyAndTree
	augmentQueue map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int
}

func RebuildNodes(ctx context.Context, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) (map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], error) {
	_ctx := ctx

	ctx = dlog.WithField(_ctx, "btrfsinspect.rebuild-nodes.step", "read-fs-data")
	dlog.Info(ctx, "Reading superblock...")
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}
	nodeGraph, keyIO, err := ScanDevices(ctx, fs, nodeScanResults) // ScanDevices does its own logging
	if err != nil {
		return nil, err
	}

	ctx = dlog.WithField(_ctx, "btrfsinspect.rebuild-nodes.step", "rebuild")
	dlog.Info(ctx, "Rebuilding node tree...")
	o := &rebuilder{
		sb: *sb,

		graph: nodeGraph,
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

func (o *rebuilder) rebuild(_ctx context.Context) error {
	// Initialize
	o.augmentQueue = make(map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int)

	// Seed the queue
	o.treeQueue = []btrfsprim.ObjID{
		btrfsprim.ROOT_TREE_OBJECTID,
		btrfsprim.CHUNK_TREE_OBJECTID,
		// btrfsprim.TREE_LOG_OBJECTID, // TODO(lukeshu): Special LOG_TREE handling
		btrfsprim.BLOCK_GROUP_TREE_OBJECTID,
	}

	for passNum := 0; len(o.treeQueue) > 0 || len(o.itemQueue) > 0 || len(o.augmentQueue) > 0; passNum++ {
		passCtx := dlog.WithField(_ctx, "btrfsinspect.rebuild-nodes.rebuild.pass", passNum)

		// Add items to the queue (drain o.treeQueue, fill o.itemQueue)
		stepCtx := dlog.WithField(passCtx, "btrfsinspect.rebuild-nodes.rebuild.substep", "collect-items")
		treeQueue := o.treeQueue
		o.treeQueue = nil
		sort.Slice(treeQueue, func(i, j int) bool {
			return treeQueue[i] < treeQueue[j]
		})
		// Because trees can be wildly different sizes, it's impossible to have a meaningful
		// progress percentage here.
		for _, treeID := range treeQueue {
			o.rebuilt.AddTree(stepCtx, treeID)
		}

		// Handle items in the queue (drain o.itemQueue, fill o.augmentQueue and o.treeQueue)
		stepCtx = dlog.WithField(passCtx, "btrfsinspect.rebuild-nodes.rebuild.substep", "process-items")
		itemQueue := o.itemQueue
		o.itemQueue = nil
		var progress textui.Portion[int]
		progress.D = len(itemQueue)
		progressWriter := textui.NewProgress[textui.Portion[int]](stepCtx, dlog.LogLevelInfo, 1*time.Second)
		stepCtx = dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.substep.progress", &progress)
		for i, key := range itemQueue {
			itemCtx := dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.process.item", key)
			progress.N = i
			progressWriter.Set(progress)
			o.curKey = key
			itemBody, ok := o.rebuilt.Load(itemCtx, key.TreeID, key.Key)
			if !ok {
				o.ioErr(itemCtx, fmt.Errorf("could not read previously read item: %v", key))
			}
			handleItem(o, itemCtx, key.TreeID, btrfstree.Item{
				Key:  key.Key,
				Body: itemBody,
			})
			if key.ItemType == btrfsitem.ROOT_ITEM_KEY {
				o.treeQueue = append(o.treeQueue, key.ObjectID)
			}
		}
		progress.N = len(itemQueue)
		progressWriter.Set(progress)
		progressWriter.Done()

		// Apply augments (drain o.augmentQueue, fill o.itemQueue)
		stepCtx = dlog.WithField(passCtx, "btrfsinspect.rebuild-nodes.rebuild.substep", "apply-augments")
		resolvedAugments := make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], len(o.augmentQueue))
		progress.N = 0
		progress.D = 0
		for _, treeID := range maps.SortedKeys(o.augmentQueue) {
			treeCtx := dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.augment.tree", treeID)
			resolvedAugments[treeID] = o.resolveTreeAugments(treeCtx, o.augmentQueue[treeID])
			progress.D += len(resolvedAugments[treeID])
		}
		o.augmentQueue = make(map[btrfsprim.ObjID][]map[btrfsvol.LogicalAddr]int)
		progressWriter = textui.NewProgress[textui.Portion[int]](stepCtx, dlog.LogLevelInfo, 1*time.Second)
		stepCtx = dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.substep.progress", &progress)
		for _, treeID := range maps.SortedKeys(resolvedAugments) {
			treeCtx := dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.augment.tree", treeID)
			for _, nodeAddr := range maps.SortedKeys(resolvedAugments[treeID]) {
				progressWriter.Set(progress)
				o.rebuilt.AddRoot(treeCtx, treeID, nodeAddr)
				progress.N++
			}
		}
		progressWriter.Set(progress)
		progressWriter.Done()
	}
	return nil
}

func (o *rebuilder) cbAddedItem(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
	o.itemQueue = append(o.itemQueue, keyAndTree{
		TreeID: tree,
		Key:    key,
	})
}

func (o *rebuilder) cbLookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.reason", "tree Root")
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.key",
		fmt.Sprintf("tree=%v key={%v %v ?}", btrfsprim.ROOT_TREE_OBJECTID, tree, btrfsitem.ROOT_ITEM_KEY))
	key, ok := o._want(ctx, btrfsprim.ROOT_TREE_OBJECTID, tree, btrfsitem.ROOT_ITEM_KEY)
	if !ok {
		o.itemQueue = append(o.itemQueue, o.curKey)
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
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.reason", "resolve parent UUID")
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.key", keyAndTree{TreeID: btrfsprim.UUID_TREE_OBJECTID, Key: key})
	if ok := o._wantOff(ctx, btrfsprim.UUID_TREE_OBJECTID, key); !ok {
		o.itemQueue = append(o.itemQueue, o.curKey)
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
			counts[item]++
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
			dlog.Infof(ctx, "lists[%d]: chose (none) from %v", i, maps.SortedKeys(list))
		} else {
			dlog.Infof(ctx, "lists[%d]: chose %v from %v", i, chose.TakeOne(), maps.SortedKeys(list))
		}
	}

	return ret
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (o *rebuilder) wantAugment(ctx context.Context, treeID btrfsprim.ObjID, choices containers.Set[btrfsvol.LogicalAddr]) {
	if len(choices) == 0 {
		dlog.Error(ctx, "could not find wanted item")
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
	dlog.Infof(ctx, "choices=%v", maps.SortedKeys(choicesWithDist))
	o.augmentQueue[treeID] = append(o.augmentQueue[treeID], choicesWithDist)
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// fsErr implements rebuildCallbacks.
func (o *rebuilder) fsErr(ctx context.Context, e error) {
	dlog.Errorf(ctx, "filesystem error: %v", e)
}

// want implements rebuildCallbacks.
func (o *rebuilder) want(ctx context.Context, reason string, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key",
		fmt.Sprintf("tree=%v key={%v %v ?}", treeID, objID, typ))
	o._want(ctx, treeID, objID, typ)
}
func (o *rebuilder) _want(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType) (key btrfsprim.Key, ok bool) {
	if !o.rebuilt.AddTree(ctx, treeID) {
		o.itemQueue = append(o.itemQueue, o.curKey)
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
func (o *rebuilder) wantOff(ctx context.Context, reason string, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, off uint64) {
	key := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   off,
	}
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key", keyAndTree{TreeID: treeID, Key: key})
	o._wantOff(ctx, treeID, key)
}
func (o *rebuilder) _wantOff(ctx context.Context, treeID btrfsprim.ObjID, tgt btrfsprim.Key) (ok bool) {
	if !o.rebuilt.AddTree(ctx, treeID) {
		o.itemQueue = append(o.itemQueue, o.curKey)
		return false
	}

	// check if we already have it

	if _, ok := o.rebuilt.Load(ctx, treeID, tgt); ok {
		return true
	}

	// OK, we need to insert it

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
func (o *rebuilder) wantFunc(ctx context.Context, reason string, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, fn func(btrfsitem.Item) bool) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key",
		fmt.Sprintf("tree=%v key={%v %v ?} +func", treeID, objID, typ))

	if !o.rebuilt.AddTree(ctx, treeID) {
		o.itemQueue = append(o.itemQueue, o.curKey)
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

	wants := make(containers.Set[btrfsvol.LogicalAddr])
	o.rebuilt.Keys(treeID).Subrange(
		func(k btrfsprim.Key, _ keyio.ItemPtr) int { k.Offset = 0; return tgt.Cmp(k) },
		func(k btrfsprim.Key, v keyio.ItemPtr) bool {
			itemBody, ok := o.keyIO.ReadItem(ctx, v)
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

func (o *rebuilder) _wantRange(
	ctx context.Context,
	treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType,
	beg, end uint64,
) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key",
		fmt.Sprintf("tree=%v key={%v %v ?}", treeID, objID, typ))

	if !o.rebuilt.AddTree(ctx, treeID) {
		o.itemQueue = append(o.itemQueue, o.curKey)
		return
	}

	sizeFn := func(key btrfsprim.Key) (uint64, error) {
		ptr, ok := o.rebuilt.Keys(treeID).Load(key)
		if !ok {
			panic(fmt.Errorf("should not happen: could not load key: %v", key))
		}
		sizeAndErr, ok := o.keyIO.Sizes[ptr]
		if !ok {
			panic(fmt.Errorf("should not happen: %v item did not have a size recorded", typ))
		}
		return sizeAndErr.Size, sizeAndErr.Err
	}

	// Step 1: Build a listing of the runs that we do have.
	runMin := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   0, // *NOT* `beg`
	}
	runMax := btrfsprim.Key{
		ObjectID: objID,
		ItemType: typ,
		Offset:   end - 1,
	}
	runKeys := o.rebuilt.SearchAll(ctx, treeID, func(key btrfsprim.Key) int {
		switch {
		case runMin.Cmp(key) < 0:
			return 1
		case runMax.Cmp(key) > 0:
			return -1
		default:
			return 0
		}
	})

	// Step 2: Build a listing of the gaps.
	//
	// Start with a gap of the whole range, then subtract each run
	// from it.
	type gap struct {
		// range is [Beg,End)
		Beg, End uint64
	}
	gaps := &containers.RBTree[containers.NativeOrdered[uint64], gap]{
		KeyFn: func(gap gap) containers.NativeOrdered[uint64] {
			return containers.NativeOrdered[uint64]{Val: gap.Beg}
		},
	}
	gaps.Insert(gap{
		Beg: beg,
		End: end,
	})
	for _, runKey := range runKeys {
		runSize, err := sizeFn(runKey)
		if err != nil {
			o.fsErr(ctx, fmt.Errorf("tree=%v key=%v: %w", treeID, runKey, err))
		}
		if runSize == 0 {
			continue
		}
		runBeg := runKey.Offset
		runEnd := runBeg + runSize
		if runEnd <= beg {
			continue
		}
		overlappingGaps := gaps.SearchRange(func(gap gap) int {
			switch {
			case gap.End <= runBeg:
				return 1
			case runEnd <= gap.Beg:
				return -1
			default:
				return 0
			}
		})
		if len(overlappingGaps) == 0 {
			continue
		}
		gapsBeg := overlappingGaps[0].Beg
		gapsEnd := overlappingGaps[len(overlappingGaps)-1].End
		for _, gap := range overlappingGaps {
			gaps.Delete(containers.NativeOrdered[uint64]{Val: gap.Beg})
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
	}

	// Step 2: Fill each gap.
	_ = gaps.Walk(func(rbNode *containers.RBNode[gap]) error {
		gap := rbNode.Value
		last := gap.Beg
		runMin := btrfsprim.Key{
			ObjectID: objID,
			ItemType: typ,
			Offset:   0, // *NOT* `gap.Beg`
		}
		runMax := btrfsprim.Key{
			ObjectID: objID,
			ItemType: typ,
			Offset:   gap.End - 1,
		}
		o.rebuilt.Keys(treeID).Subrange(
			func(key btrfsprim.Key, _ keyio.ItemPtr) int {
				switch {
				case runMin.Cmp(key) < 0:
					return 1
				case runMax.Cmp(key) > 0:
					return -1
				default:
					return 0
				}
			},
			func(k btrfsprim.Key, v keyio.ItemPtr) bool {
				runSize, err := sizeFn(k)
				if err != nil {
					o.fsErr(ctx, fmt.Errorf("tree=%v key=%v: %w", treeID, k, err))
					return true
				}
				if runSize == 0 {
					return true
				}
				runBeg := k.Offset
				runEnd := runBeg + runSize
				if runEnd <= gap.Beg {
					return true
				}

				// TODO: This is dumb and greedy.
				if last < runBeg {
					// log an error
					wantCtx := dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key",
						fmt.Sprintf("tree=%v key={%v %v %v-%v}", treeID, objID, typ, last, runBeg))
					o.wantAugment(wantCtx, treeID, nil)
				}
				wantCtx := dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key",
					fmt.Sprintf("tree=%v key={%v %v %v-%v}", treeID, objID, typ, gap.Beg, gap.End))
				o.wantAugment(wantCtx, treeID, o.rebuilt.LeafToRoots(ctx, treeID, v.Node))
				last = runEnd

				return true
			})
		if last < gap.End {
			// log an error
			wantCtx := dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.key",
				fmt.Sprintf("tree=%v key={%v, %v, %v-%v}",
					treeID, objID, typ, last, gap.End))
			o.wantAugment(wantCtx, treeID, nil)
		}
		return nil
	})
}

// func implements rebuildCallbacks.
//
// interval is [beg, end)
func (o *rebuilder) wantCSum(ctx context.Context, reason string, beg, end btrfsvol.LogicalAddr) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.want.reason", reason)
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
