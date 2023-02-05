// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
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

func (a keyAndTree) Compare(b keyAndTree) int {
	if d := containers.NativeCompare(a.TreeID, b.TreeID); d != 0 {
		return d
	}
	return a.Key.Compare(b.Key)
}

func (o keyAndTree) String() string {
	return fmt.Sprintf("tree=%v key=%v", o.TreeID, o.Key)
}

type rebuilder struct {
	sb    btrfstree.Superblock
	graph graph.Graph
	keyIO *keyio.Handle

	rebuilt *btrees.RebuiltForrest

	curKey             keyAndTree
	treeQueue          containers.Set[btrfsprim.ObjID]
	itemQueue          containers.Set[keyAndTree]
	augmentQueue       map[btrfsprim.ObjID]*treeAugmentQueue
	numAugments        int
	numAugmentFailures int
}

type treeAugmentQueue struct {
	keyBuf strings.Builder
	zero   map[string]struct{}
	single map[string]btrfsvol.LogicalAddr
	multi  map[string]containers.Set[btrfsvol.LogicalAddr]
}

type Rebuilder interface {
	Rebuild(context.Context) error
	ListRoots() map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]
}

func NewRebuilder(ctx context.Context, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) (Rebuilder, error) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.step", "read-fs-data")
	sb, nodeGraph, keyIO, err := ScanDevices(ctx, fs, nodeScanResults) // ScanDevices does its own logging
	if err != nil {
		return nil, err
	}

	o := &rebuilder{
		sb:    sb,
		graph: nodeGraph,
		keyIO: keyIO,
	}
	o.rebuilt = btrees.NewRebuiltForrest(sb, nodeGraph, keyIO,
		o.cbAddedItem, o.cbLookupRoot, o.cbLookupUUID)
	return o, nil
}

func (o *rebuilder) ioErr(ctx context.Context, err error) {
	err = fmt.Errorf("should not happen: i/o error: %w", err)
	dlog.Error(ctx, err)
	panic(err)
}

func (o *rebuilder) ListRoots() map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr] {
	return o.rebuilt.ListRoots()
}

type itemStats struct {
	textui.Portion[int]
	NumAugments     int
	NumFailures     int
	NumAugmentTrees int
}

func (s itemStats) String() string {
	// return textui.Sprintf("%v (queued %v augments and %v failures across %v trees)",
	return textui.Sprintf("%v (aug:%v fail:%v trees:%v)",
		s.Portion, s.NumAugments, s.NumFailures, s.NumAugmentTrees)
}

func (o *rebuilder) Rebuild(_ctx context.Context) error {
	_ctx = dlog.WithField(_ctx, "btrfsinspect.rebuild-nodes.step", "rebuild")

	// Initialize
	o.itemQueue = make(containers.Set[keyAndTree])
	o.augmentQueue = make(map[btrfsprim.ObjID]*treeAugmentQueue)

	// Seed the queue
	o.treeQueue = containers.NewSet[btrfsprim.ObjID](
		btrfsprim.ROOT_TREE_OBJECTID,
		btrfsprim.CHUNK_TREE_OBJECTID,
		// btrfsprim.TREE_LOG_OBJECTID, // TODO(lukeshu): Special LOG_TREE handling
		btrfsprim.BLOCK_GROUP_TREE_OBJECTID,
	)

	for passNum := 0; len(o.treeQueue) > 0 || len(o.itemQueue) > 0 || len(o.augmentQueue) > 0; passNum++ {
		passCtx := dlog.WithField(_ctx, "btrfsinspect.rebuild-nodes.rebuild.pass", passNum)

		// Add items to the queue (drain o.treeQueue, fill o.itemQueue)
		if true {
			stepCtx := dlog.WithField(passCtx, "btrfsinspect.rebuild-nodes.rebuild.substep", "collect-items")
			treeQueue := o.treeQueue
			o.treeQueue = make(containers.Set[btrfsprim.ObjID])
			// Because trees can be wildly different sizes, it's impossible to have a meaningful
			// progress percentage here.
			for _, treeID := range maps.SortedKeys(treeQueue) {
				if err := _ctx.Err(); err != nil {
					return err
				}
				o.curKey = keyAndTree{TreeID: treeID}
				o.rebuilt.Tree(stepCtx, treeID)
			}
		}
		runtime.GC()

		// Handle items in the queue (drain o.itemQueue, fill o.augmentQueue and o.treeQueue)
		if true {
			stepCtx := dlog.WithField(passCtx, "btrfsinspect.rebuild-nodes.rebuild.substep", "process-items")
			itemQueue := maps.Keys(o.itemQueue)
			o.itemQueue = make(containers.Set[keyAndTree])
			sort.Slice(itemQueue, func(i, j int) bool {
				return itemQueue[i].Compare(itemQueue[j]) < 0
			})
			var progress itemStats
			progress.D = len(itemQueue)
			progressWriter := textui.NewProgress[itemStats](stepCtx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
			stepCtx = dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.substep.progress", &progress)
			type keyAndBody struct {
				keyAndTree
				Body btrfsitem.Item
			}
			itemChan := make(chan keyAndBody, textui.Tunable(300)) // average items-per-node≈100; let's have a buffer of ~3 nodes
			grp := dgroup.NewGroup(stepCtx, dgroup.GroupConfig{})
			grp.Go("io", func(stepCtx context.Context) error {
				defer close(itemChan)
				for _, key := range itemQueue {
					if err := stepCtx.Err(); err != nil {
						return err
					}
					itemCtx := dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.process.item", key)
					itemBody, ok := o.rebuilt.Tree(itemCtx, key.TreeID).ReadItem(itemCtx, key.Key)
					if !ok {
						o.ioErr(itemCtx, fmt.Errorf("could not read previously read item: %v", key))
					}
					itemChan <- keyAndBody{
						keyAndTree: key,
						Body:       itemBody,
					}
				}
				return nil
			})
			grp.Go("cpu", func(stepCtx context.Context) error {
				defer progressWriter.Done()
				for item := range itemChan {
					itemCtx := dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.process.item", item.keyAndTree)
					o.curKey = item.keyAndTree
					handleItem(o, itemCtx, item.TreeID, btrfstree.Item{
						Key:  item.Key,
						Body: item.Body,
					})
					if item.ItemType == btrfsitem.ROOT_ITEM_KEY {
						o.treeQueue.Insert(item.ObjectID)
					}
					progress.N++
					progress.NumAugments = o.numAugments
					progress.NumFailures = o.numAugmentFailures
					progress.NumAugmentTrees = len(o.augmentQueue)
					progressWriter.Set(progress)
				}
				return nil
			})
			if err := grp.Wait(); err != nil {
				return err
			}
		}
		runtime.GC()

		// Apply augments (drain o.augmentQueue, fill o.itemQueue)
		if true {
			stepCtx := dlog.WithField(passCtx, "btrfsinspect.rebuild-nodes.rebuild.substep", "apply-augments")
			resolvedAugments := make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], len(o.augmentQueue))
			var progress textui.Portion[int]
			for _, treeID := range maps.SortedKeys(o.augmentQueue) {
				if err := _ctx.Err(); err != nil {
					return err
				}
				treeCtx := dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.augment.tree", treeID)
				resolvedAugments[treeID] = o.resolveTreeAugments(treeCtx, treeID)
				progress.D += len(resolvedAugments[treeID])
			}
			o.augmentQueue = make(map[btrfsprim.ObjID]*treeAugmentQueue)
			o.numAugments = 0
			o.numAugmentFailures = 0
			runtime.GC()
			progressWriter := textui.NewProgress[textui.Portion[int]](stepCtx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
			stepCtx = dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.substep.progress", &progress)
			for _, treeID := range maps.SortedKeys(resolvedAugments) {
				treeCtx := dlog.WithField(stepCtx, "btrfsinspect.rebuild-nodes.rebuild.augment.tree", treeID)
				for _, nodeAddr := range maps.SortedKeys(resolvedAugments[treeID]) {
					if err := _ctx.Err(); err != nil {
						progressWriter.Set(progress)
						progressWriter.Done()
						return err
					}
					progressWriter.Set(progress)
					o.rebuilt.Tree(treeCtx, treeID).AddRoot(treeCtx, nodeAddr)
					progress.N++
				}
			}
			progressWriter.Set(progress)
			progressWriter.Done()
		}
		runtime.GC()
	}
	return nil
}

func (o *rebuilder) enqueueRetry() {
	if o.curKey.Key == (btrfsprim.Key{}) {
		o.treeQueue.Insert(o.curKey.TreeID)
	} else {
		o.itemQueue.Insert(o.curKey)
	}
}

func (o *rebuilder) cbAddedItem(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
	if handleWouldBeNoOp(key.ItemType) {
		return
	}
	o.itemQueue.Insert(keyAndTree{
		TreeID: tree,
		Key:    key,
	})
}

func (o *rebuilder) cbLookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.reason", "tree Root")
	key := keyAndTree{
		TreeID: btrfsprim.ROOT_TREE_OBJECTID,
		Key: btrfsprim.Key{
			ObjectID: tree,
			ItemType: btrfsitem.ROOT_ITEM_KEY,
		},
	}
	wantKey := fmt.Sprintf("tree=%v key={%v %v ?}", key.TreeID, key.ObjectID, key.ItemType)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.key", wantKey)
	key.Key, ok = o._want(ctx, key.TreeID, wantKey, key.ObjectID, key.ItemType)
	if !ok {
		o.enqueueRetry()
		return 0, btrfsitem.Root{}, false
	}
	itemBody, ok := o.rebuilt.Tree(ctx, key.TreeID).ReadItem(ctx, key.Key)
	if !ok {
		o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v", key))
	}
	switch itemBody := itemBody.(type) {
	case btrfsitem.Root:
		return btrfsprim.Generation(key.Offset), itemBody, true
	case btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: %v: %w", key, itemBody.Err))
		return 0, btrfsitem.Root{}, false
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

func (o *rebuilder) cbLookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.reason", "resolve parent UUID")
	key := keyAndTree{TreeID: btrfsprim.UUID_TREE_OBJECTID, Key: btrfsitem.UUIDToKey(uuid)}
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.key", key.String())
	if !o._wantOff(ctx, key.TreeID, key.String(), key.Key) {
		o.enqueueRetry()
		return 0, false
	}
	itemBody, ok := o.rebuilt.Tree(ctx, key.TreeID).ReadItem(ctx, key.Key)
	if !ok {
		o.ioErr(ctx, fmt.Errorf("could not read previously read item: %v", key))
	}
	switch itemBody := itemBody.(type) {
	case btrfsitem.UUIDMap:
		return itemBody.ObjID, true
	case btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: %v: %w", key, itemBody.Err))
		return 0, false
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}

func (o *rebuilder) resolveTreeAugments(ctx context.Context, treeID btrfsprim.ObjID) containers.Set[btrfsvol.LogicalAddr] {
	// Define an algorithm that takes several lists of items, and returns a
	// set of those items such that each input list contains zero or one of
	// the items from your return set.  The same item may appear in multiple
	// of the input lists.

	type ChoiceInfo struct {
		Count      int
		Distance   int
		Generation btrfsprim.Generation
	}
	choices := make(map[btrfsvol.LogicalAddr]ChoiceInfo)
	// o.augmentQueue[treeID].zero is optimized storage for lists
	// with zero items.  Go ahead and free that memory up.
	o.augmentQueue[treeID].zero = nil
	// o.augmentQueue[treeID].single is optimized storage for
	// lists with exactly 1 item.
	for _, choice := range o.augmentQueue[treeID].single {
		if old, ok := choices[choice]; ok {
			old.Count++
			choices[choice] = old
		} else {
			choices[choice] = ChoiceInfo{
				Count:      1,
				Distance:   discardOK(o.rebuilt.Tree(ctx, treeID).COWDistance(o.graph.Nodes[choice].Owner)),
				Generation: o.graph.Nodes[choice].Generation,
			}
		}
	}
	// o.augmentQueue[treeID].multi is the main list storage.
	for _, list := range o.augmentQueue[treeID].multi {
		for choice := range list {
			if old, ok := choices[choice]; ok {
				old.Count++
				choices[choice] = old
			} else {
				choices[choice] = ChoiceInfo{
					Count:      1,
					Distance:   discardOK(o.rebuilt.Tree(ctx, treeID).COWDistance(o.graph.Nodes[choice].Owner)),
					Generation: o.graph.Nodes[choice].Generation,
				}
			}
		}
	}

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
	// > legal solution would be `[]`, `[A]` or `[B]`.  It would not be legal
	// > to return `[A, B]`.
	//
	// The algorithm should optimize for the following goals:
	//
	//  - We prefer that each input list have an item in the return set.
	//
	//    > In Example 1, while `[]`, `[B]`, and `[C]` are permissible
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
	// The relative priority of these 4 goals is undefined; preferably the
	// algorithm should be defined in a way that makes it easy to adjust the
	// relative priorities.

	ret := make(containers.Set[btrfsvol.LogicalAddr])
	illegal := make(containers.Set[btrfsvol.LogicalAddr]) // cannot-be-accepted and already-accepted
	accept := func(item btrfsvol.LogicalAddr) {
		ret.Insert(item)
		for _, list := range o.augmentQueue[treeID].multi {
			if list.Has(item) {
				illegal.InsertFrom(list)
			}
		}
	}

	sortedItems := maps.Keys(choices)
	sort.Slice(sortedItems, func(i, j int) bool {
		iItem, jItem := sortedItems[i], sortedItems[j]
		if choices[iItem].Count != choices[jItem].Count {
			return choices[iItem].Count > choices[jItem].Count // reverse this check; higher counts should sort lower
		}
		if choices[iItem].Distance != choices[jItem].Distance {
			return choices[iItem].Distance < choices[jItem].Distance
		}
		if choices[iItem].Generation != choices[jItem].Generation {
			return choices[iItem].Generation > choices[jItem].Generation // reverse this check; higher generations should sort lower
		}
		return iItem < jItem // laddr is as good a tiebreaker as anything
	})
	for _, item := range sortedItems {
		if !illegal.Has(item) {
			accept(item)
		}
	}

	// Log our result
	wantKeys := append(
		maps.Keys(o.augmentQueue[treeID].single),
		maps.Keys(o.augmentQueue[treeID].multi)...)
	sort.Strings(wantKeys)
	for _, wantKey := range wantKeys {
		list, ok := o.augmentQueue[treeID].multi[wantKey]
		if !ok {
			list = containers.NewSet[btrfsvol.LogicalAddr](o.augmentQueue[treeID].single[wantKey])
		}
		chose := list.Intersection(ret)
		if len(chose) == 0 {
			dlog.Infof(ctx, "lists[%q]: chose (none) from %v", wantKey, maps.SortedKeys(list))
		} else {
			dlog.Infof(ctx, "lists[%q]: chose %v from %v", wantKey, chose.TakeOne(), maps.SortedKeys(list))
		}
	}

	// Free some memory
	o.augmentQueue[treeID].single = nil
	o.augmentQueue[treeID].multi = nil
	o.augmentQueue[treeID].keyBuf.Reset()

	return ret
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func shortenWantKey(wantKey string) string {
	if !strings.HasPrefix(wantKey, "tree=") {
		panic("should not happen")
	}
	sp := strings.IndexByte(wantKey, ' ')
	if sp < 0 {
		panic("should not happen")
	}
	return wantKey[sp+1:]
}

func (treeQueue *treeAugmentQueue) has(wantKey string) bool {
	if treeQueue != nil {
		wantKey = shortenWantKey(wantKey)
		if treeQueue.zero != nil {
			if _, ok := treeQueue.zero[wantKey]; ok {
				return true
			}
		}
		if treeQueue.single != nil {
			if _, ok := treeQueue.single[wantKey]; ok {
				return true
			}
		}
		if treeQueue.multi != nil {
			if _, ok := treeQueue.multi[wantKey]; ok {
				return true
			}
		}
	}
	return false
}

func (treeQueue *treeAugmentQueue) store(wantKey string, choices containers.Set[btrfsvol.LogicalAddr]) {
	if len(choices) == 0 && (strings.Contains(wantKey, " name=") || strings.Contains(wantKey, "-")) {
		// This wantKey is unlikely to come up again, so it's not worth storing a negative result.
		return
	}
	wantKey = shortenWantKey(wantKey)
	beg := treeQueue.keyBuf.Len()
	if treeQueue.keyBuf.Cap()-beg < len(wantKey) {
		treeQueue.keyBuf.Reset()
		treeQueue.keyBuf.Grow(textui.Tunable(4096))
		beg = 0
	}
	treeQueue.keyBuf.WriteString(wantKey)
	wantKey = treeQueue.keyBuf.String()[beg:]

	switch len(choices) {
	case 0:
		if treeQueue.zero == nil {
			treeQueue.zero = make(map[string]struct{})
		}
		treeQueue.zero[wantKey] = struct{}{}
	case 1:
		if treeQueue.single == nil {
			treeQueue.single = make(map[string]btrfsvol.LogicalAddr)
		}
		treeQueue.single[wantKey] = choices.TakeOne()
	default:
		if treeQueue.multi == nil {
			treeQueue.multi = make(map[string]containers.Set[btrfsvol.LogicalAddr])
		}
		treeQueue.multi[wantKey] = choices
	}
}

func (o *rebuilder) hasAugment(treeID btrfsprim.ObjID, wantKey string) bool {
	return o.augmentQueue[treeID].has(wantKey)
}

func (o *rebuilder) wantAugment(ctx context.Context, treeID btrfsprim.ObjID, wantKey string, choices containers.Set[btrfsvol.LogicalAddr]) {
	if o.augmentQueue[treeID] == nil {
		o.augmentQueue[treeID] = new(treeAugmentQueue)
	}
	o.augmentQueue[treeID].store(wantKey, choices)
	if len(choices) == 0 {
		dlog.Error(ctx, "could not find wanted item")
		o.numAugmentFailures++
	} else {
		dlog.Infof(ctx, "choices=%v", maps.SortedKeys(choices))
		o.numAugments++
	}
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
