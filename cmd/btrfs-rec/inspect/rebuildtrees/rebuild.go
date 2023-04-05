// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package rebuildtrees is the guts of the `btrfs-rec inspect
// rebuild-trees` command, which rebuilds broken trees, but requires
// already-functioning chunk/dev-extent/blockgroup trees.
// chunk/dev-extent/blockgroup trees.
package rebuildtrees

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"time"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfscheck"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
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
	scan ScanDevicesResult

	rebuilt *btrfsutil.RebuiltForrest

	curKey struct {
		TreeID btrfsprim.ObjID
		Key    containers.Optional[btrfsprim.Key]
	}
	treeQueue          containers.Set[btrfsprim.ObjID]
	retryItemQueue     map[btrfsprim.ObjID]containers.Set[keyAndTree]
	addedItemQueue     containers.Set[keyAndTree]
	settledItemQueue   containers.Set[keyAndTree]
	augmentQueue       map[btrfsprim.ObjID]*treeAugmentQueue
	numAugments        int
	numAugmentFailures int
}

type treeAugmentQueue struct {
	zero   map[want]struct{}
	single map[want]btrfsvol.LogicalAddr
	multi  map[want]containers.Set[btrfsvol.LogicalAddr]
}

type Rebuilder interface {
	Rebuild(context.Context) error
	ListRoots(context.Context) map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]
}

func NewRebuilder(ctx context.Context, fs *btrfs.FS, nodeList []btrfsvol.LogicalAddr) (Rebuilder, error) {
	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.step", "read-fs-data")
	scanData, err := ScanDevices(ctx, fs, nodeList) // ScanDevices does its own logging
	if err != nil {
		return nil, err
	}

	o := &rebuilder{
		scan: scanData,
	}
	o.rebuilt = btrfsutil.NewRebuiltForrest(fs, scanData.Graph, forrestCallbacks{o})
	return o, nil
}

func (o *rebuilder) ListRoots(ctx context.Context) map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr] {
	return o.rebuilt.RebuiltListRoots(ctx)
}

func (o *rebuilder) Rebuild(ctx context.Context) error {
	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.step", "rebuild")

	// Initialize
	o.retryItemQueue = make(map[btrfsprim.ObjID]containers.Set[keyAndTree])
	o.addedItemQueue = make(containers.Set[keyAndTree])
	o.settledItemQueue = make(containers.Set[keyAndTree])
	o.augmentQueue = make(map[btrfsprim.ObjID]*treeAugmentQueue)

	// Seed the queue
	o.treeQueue = containers.NewSet[btrfsprim.ObjID](
		btrfsprim.ROOT_TREE_OBJECTID,
		btrfsprim.CHUNK_TREE_OBJECTID,
		// btrfsprim.TREE_LOG_OBJECTID, // TODO(lukeshu): Special LOG_TREE handling
		btrfsprim.BLOCK_GROUP_TREE_OBJECTID,
	)

	// Run
	for passNum := 0; len(o.treeQueue) > 0 || len(o.addedItemQueue) > 0 || len(o.settledItemQueue) > 0 || len(o.augmentQueue) > 0; passNum++ {
		ctx := dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.pass", passNum)

		// Crawl trees (Drain o.treeQueue, fill o.addedItemQueue).
		if err := o.processTreeQueue(ctx); err != nil {
			return err
		}
		runtime.GC()

		if len(o.addedItemQueue) > 0 {
			// Settle items (drain o.addedItemQueue, fill o.augmentQueue and o.settledItemQueue).
			if err := o.processAddedItemQueue(ctx); err != nil {
				return err
			}
		} else {
			// Process items (drain o.settledItemQueue, fill o.augmentQueue and o.treeQueue).
			if err := o.processSettledItemQueue(ctx); err != nil {
				return err
			}
		}
		runtime.GC()

		// Apply augments (drain o.augmentQueue (and maybe o.retryItemQueue), fill o.addedItemQueue).
		if err := o.processAugmentQueue(ctx); err != nil {
			return err
		}
		runtime.GC()
	}

	return nil
}

// processTreeQueue drains o.treeQueue, filling o.addedItemQueue.
func (o *rebuilder) processTreeQueue(ctx context.Context) error {
	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.substep", "collect-items")

	queue := maps.SortedKeys(o.treeQueue)
	o.treeQueue = make(containers.Set[btrfsprim.ObjID])

	// Because trees can be wildly different sizes, it's impossible to have a meaningful
	// progress percentage here.
	o.curKey.Key.OK = false
	for _, o.curKey.TreeID = range queue {
		if err := ctx.Err(); err != nil {
			return err
		}
		// This will call o.AddedItem as nescessary, which
		// inserts to o.addedItemQueue.
		_ = o.rebuilt.RebuiltTree(ctx, o.curKey.TreeID)
	}

	return nil
}

type settleItemStats struct {
	textui.Portion[int]
	NumAugments     int
	NumAugmentTrees int
}

func (s settleItemStats) String() string {
	// return textui.Sprintf("%v (queued %v augments across %v trees)",
	return textui.Sprintf("%v (aug:%v trees:%v)",
		s.Portion, s.NumAugments, s.NumAugmentTrees)
}

// processAddedItemQueue drains o.addedItemQueue, filling o.augmentQueue and o.settledItemQueue.
func (o *rebuilder) processAddedItemQueue(ctx context.Context) error {
	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.substep", "settle-items")

	queue := maps.Keys(o.addedItemQueue)
	o.addedItemQueue = make(containers.Set[keyAndTree])
	sort.Slice(queue, func(i, j int) bool {
		return queue[i].Compare(queue[j]) < 0
	})

	var progress settleItemStats
	progress.D = len(queue)
	progressWriter := textui.NewProgress[settleItemStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	progressWriter.Set(progress)
	defer progressWriter.Done()

	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.substep.progress", &progress)

	for _, key := range queue {
		ctx := dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.settle.item", key)
		tree := o.rebuilt.RebuiltTree(ctx, key.TreeID)
		incPtr, ok := tree.RebuiltAcquireItems(ctx).Load(key.Key)
		tree.RebuiltReleaseItems()
		if !ok {
			panic(fmt.Errorf("should not happen: failed to load already-added item: %v", key))
		}
		excPtr, ok := tree.RebuiltAcquirePotentialItems(ctx).Load(key.Key)
		tree.RebuiltReleasePotentialItems()
		if ok && tree.RebuiltShouldReplace(incPtr.Node, excPtr.Node) {
			wantKey := wantWithTree{
				TreeID: key.TreeID,
				Key:    wantFromKey(key.Key),
			}
			o.wantAugment(ctx, wantKey, tree.RebuiltLeafToRoots(ctx, excPtr.Node))
			progress.NumAugments = o.numAugments
			progress.NumAugmentTrees = len(o.augmentQueue)
		} else if !btrfscheck.HandleItemWouldBeNoOp(key.ItemType) {
			o.settledItemQueue.Insert(key)
		}

		progress.N++
		progressWriter.Set(progress)
	}

	return nil
}

type processItemStats struct {
	textui.Portion[int]
	NumAugments     int
	NumFailures     int
	NumAugmentTrees int
}

func (s processItemStats) String() string {
	// return textui.Sprintf("%v (queued %v augments and %v failures across %v trees)",
	return textui.Sprintf("%v (aug:%v fail:%v trees:%v)",
		s.Portion, s.NumAugments, s.NumFailures, s.NumAugmentTrees)
}

// processSettledItemQueue drains o.settledItemQueue, filling o.augmentQueue and o.treeQueue.
func (o *rebuilder) processSettledItemQueue(ctx context.Context) error {
	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.substep", "process-items")

	queue := maps.Keys(o.settledItemQueue)
	o.settledItemQueue = make(containers.Set[keyAndTree])
	sort.Slice(queue, func(i, j int) bool {
		return queue[i].Compare(queue[j]) < 0
	})

	var progress processItemStats
	progress.D = len(queue)
	progressWriter := textui.NewProgress[processItemStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	progressWriter.Set(progress)
	defer progressWriter.Done()

	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.substep.progress", &progress)

	type keyAndBody struct {
		keyAndTree
		Body btrfsitem.Item
	}
	itemChan := make(chan keyAndBody, textui.Tunable(300)) // average items-per-node≈100; let's have a buffer of ~3 nodes
	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
	grp.Go("io", func(ctx context.Context) error {
		defer close(itemChan)
		for _, key := range queue {
			if err := ctx.Err(); err != nil {
				return err
			}
			ctx := dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.process.item", key)
			item := keyAndBody{
				keyAndTree: key,
				Body:       o.rebuilt.RebuiltTree(ctx, key.TreeID).ReadItem(ctx, key.Key),
			}
			select {
			case itemChan <- item:
			case <-ctx.Done():
			}
		}
		return nil
	})
	grp.Go("cpu", func(ctx context.Context) error {
		o.curKey.Key.OK = true
		for item := range itemChan {
			ctx := dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.process.item", item.keyAndTree)
			o.curKey.TreeID = item.TreeID
			o.curKey.Key.Val = item.Key
			btrfscheck.HandleItem(ctx, graphCallbacks{o}, item.TreeID, btrfstree.Item{
				Key:  item.Key,
				Body: item.Body,
			})
			item.Body.Free()
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
	return grp.Wait()
}

// processAugmentQueue drains o.augmentQueue (and maybe o.retryItemQueue), filling o.addedItemQueue.
func (o *rebuilder) processAugmentQueue(ctx context.Context) error {
	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.substep", "apply-augments")

	resolvedAugments := make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], len(o.augmentQueue))
	var progress textui.Portion[int]
	for _, treeID := range maps.SortedKeys(o.augmentQueue) {
		if err := ctx.Err(); err != nil {
			return err
		}
		ctx := dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.augment.tree", treeID)
		resolvedAugments[treeID] = o.resolveTreeAugments(ctx, treeID)
		progress.D += len(resolvedAugments[treeID])
	}
	o.augmentQueue = make(map[btrfsprim.ObjID]*treeAugmentQueue)
	o.numAugments = 0
	o.numAugmentFailures = 0
	runtime.GC()

	progressWriter := textui.NewProgress[textui.Portion[int]](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	progressWriter.Set(progress)
	defer progressWriter.Done()

	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.substep.progress", &progress)
	for _, treeID := range maps.SortedKeys(resolvedAugments) {
		ctx := dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.rebuild.augment.tree", treeID)
		for _, nodeAddr := range maps.SortedKeys(resolvedAugments[treeID]) {
			if err := ctx.Err(); err != nil {
				return err
			}
			// This will call o.AddedItem as nescessary, which
			// inserts to o.addedItemQueue.
			o.rebuilt.RebuiltTree(ctx, treeID).RebuiltAddRoot(ctx, nodeAddr)
			progress.N++
			progressWriter.Set(progress)
		}
	}

	return nil
}

func (o *rebuilder) enqueueRetry(ifTreeID btrfsprim.ObjID) {
	if o.curKey.Key.OK {
		if o.retryItemQueue[ifTreeID] == nil {
			o.retryItemQueue[ifTreeID] = make(containers.Set[keyAndTree])
		}
		o.retryItemQueue[ifTreeID].Insert(keyAndTree{
			TreeID: o.curKey.TreeID,
			Key:    o.curKey.Key.Val,
		})
	} else {
		o.treeQueue.Insert(o.curKey.TreeID)
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
				Distance:   discardOK(o.rebuilt.RebuiltTree(ctx, treeID).RebuiltCOWDistance(o.scan.Graph.Nodes[choice].Owner)),
				Generation: o.scan.Graph.Nodes[choice].Generation,
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
					Distance:   discardOK(o.rebuilt.RebuiltTree(ctx, treeID).RebuiltCOWDistance(o.scan.Graph.Nodes[choice].Owner)),
					Generation: o.scan.Graph.Nodes[choice].Generation,
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
	sort.Slice(wantKeys, func(i, j int) bool {
		return wantKeys[i].Compare(wantKeys[j]) < 0
	})
	for _, wantKey := range wantKeys {
		list, ok := o.augmentQueue[treeID].multi[wantKey]
		if !ok {
			list = containers.NewSet[btrfsvol.LogicalAddr](o.augmentQueue[treeID].single[wantKey])
		}
		chose := list.Intersection(ret)
		switch {
		case len(chose) == 0:
			dlog.Infof(ctx, "lists[%q]: chose (none) from %v", wantKey, maps.SortedKeys(list))
		case len(list) > 1:
			dlog.Infof(ctx, "lists[%q]: chose %v from %v", wantKey, chose.TakeOne(), maps.SortedKeys(list))
		default:
			dlog.Debugf(ctx, "lists[%q]: chose %v from %v", wantKey, chose.TakeOne(), maps.SortedKeys(list))
		}
	}

	// Free some memory
	o.augmentQueue[treeID].single = nil
	o.augmentQueue[treeID].multi = nil

	return ret
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func (queue *treeAugmentQueue) has(wantKey want) bool {
	if queue == nil {
		return false
	}
	return (queue.zero != nil && maps.HasKey(queue.zero, wantKey)) ||
		(queue.single != nil && maps.HasKey(queue.single, wantKey)) ||
		(queue.multi != nil && maps.HasKey(queue.multi, wantKey))
}

func (queue *treeAugmentQueue) store(wantKey want, choices containers.Set[btrfsvol.LogicalAddr]) {
	if len(choices) == 0 && wantKey.OffsetType > offsetExact {
		// This wantKey is unlikely to come up again, so it's
		// not worth the RAM of storing a negative result.
		return
	}
	switch len(choices) {
	case 0:
		if queue.zero == nil {
			queue.zero = make(map[want]struct{})
		}
		queue.zero[wantKey] = struct{}{}
	case 1:
		if queue.single == nil {
			queue.single = make(map[want]btrfsvol.LogicalAddr)
		}
		queue.single[wantKey] = choices.TakeOne()
	default:
		if queue.multi == nil {
			queue.multi = make(map[want]containers.Set[btrfsvol.LogicalAddr])
		}
		queue.multi[wantKey] = choices
	}
}

func (o *rebuilder) hasAugment(wantKey wantWithTree) bool {
	return o.augmentQueue[wantKey.TreeID].has(wantKey.Key)
}

func (o *rebuilder) wantAugment(ctx context.Context, wantKey wantWithTree, choices containers.Set[btrfsvol.LogicalAddr]) {
	if o.augmentQueue[wantKey.TreeID] == nil {
		o.augmentQueue[wantKey.TreeID] = new(treeAugmentQueue)
	}
	o.augmentQueue[wantKey.TreeID].store(wantKey.Key, choices)
	if len(choices) == 0 {
		o.numAugmentFailures++
		dlog.Debug(ctx, "ERR: could not find wanted item")
	} else {
		o.numAugments++
		dlog.Debugf(ctx, "choices=%v", maps.SortedKeys(choices))
	}
}
