// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type RebuiltTree struct {
	// static

	rootErr      error
	ancestorLoop bool
	ancestorRoot btrfsprim.ObjID

	ID        btrfsprim.ObjID
	UUID      btrfsprim.UUID
	Root      btrfsvol.LogicalAddr
	Parent    *RebuiltTree
	ParentGen btrfsprim.Generation // offset of this tree's root item
	parentErr error
	forrest   *RebuiltForrest

	// mutable

	initRootsOnce sync.Once

	mu sync.RWMutex

	Roots containers.Set[btrfsvol.LogicalAddr]

	// There are 4 more mutable "members" that are protected by
	// `mu`; but they live in a shared Cache.  They are all
	// derived from tree.Roots, which is why it's OK if they get
	// evicted.
	//
	//  1. tree.acquireNodeIndex()             = tree.forrest.nodeIndex.Acquire(tree.ID)
	//  2. tree.RebuiltAcquireItems()          = tree.forrest.incItems.Acquire(tree.ID)
	//  3. tree.RebuiltAcquirePotentialItems() = tree.forrest.excItems.Acquire(tree.ID)
	//  4. tree.addErrs()                      = tree.forrest.errors.Acquire(tree.ID)
}

type rebuiltSharedCache struct {
	nodeIndex containers.Cache[btrfsprim.ObjID, rebuiltNodeIndex]
	incItems  containers.Cache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]]
	excItems  containers.Cache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]]
	errors    containers.Cache[btrfsprim.ObjID, containers.IntervalTree[btrfsprim.Key, rebuiltTreeError]]
}

func makeRebuiltSharedCache(forrest *RebuiltForrest) rebuiltSharedCache {
	var ret rebuiltSharedCache
	ret.nodeIndex = containers.NewARCache[btrfsprim.ObjID, rebuiltNodeIndex](
		textui.Tunable(8),
		containers.SourceFunc[btrfsprim.ObjID, rebuiltNodeIndex](
			func(ctx context.Context, treeID btrfsprim.ObjID, index *rebuiltNodeIndex) {
				*index = forrest.trees[treeID].uncachedNodeIndex(ctx)
			}))
	ret.incItems = containers.NewARCache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]](
		textui.Tunable(8),
		containers.SourceFunc[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]](
			func(ctx context.Context, treeID btrfsprim.ObjID, incItems *containers.SortedMap[btrfsprim.Key, ItemPtr]) {
				*incItems = forrest.trees[treeID].uncachedIncItems(ctx)
			}))
	ret.excItems = containers.NewARCache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]](
		textui.Tunable(8),
		containers.SourceFunc[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]](
			func(ctx context.Context, treeID btrfsprim.ObjID, excItems *containers.SortedMap[btrfsprim.Key, ItemPtr]) {
				*excItems = forrest.trees[treeID].uncachedExcItems(ctx)
			}))
	ret.errors = containers.NewARCache[btrfsprim.ObjID, containers.IntervalTree[btrfsprim.Key, rebuiltTreeError]](
		textui.Tunable(8),
		containers.SourceFunc[btrfsprim.ObjID, containers.IntervalTree[btrfsprim.Key, rebuiltTreeError]](
			func(ctx context.Context, treeID btrfsprim.ObjID, errs *containers.IntervalTree[btrfsprim.Key, rebuiltTreeError]) {
				*errs = forrest.trees[treeID].uncachedErrors(ctx)
			}))
	return ret
}

func (tree *RebuiltTree) initRoots(ctx context.Context) {
	tree.initRootsOnce.Do(func() {
		if tree.Root != 0 {
			tree.RebuiltAddRoot(ctx, tree.Root)
		}
	})
}

// evictable member 1: .acquireNodeIndex() /////////////////////////////////////////////////////////////////////////////

type rebuiltRoots = map[btrfsvol.LogicalAddr]rebuiltPathInfo

type rebuiltPathInfo struct {
	loMaxItem btrfsprim.Key
	hiMaxItem btrfsprim.Key
}

type rebuiltNodeIndex struct {
	// idToTree contains this tree, and all of its ancestor trees.
	idToTree map[btrfsprim.ObjID]*RebuiltTree

	// nodeToRoots contains all nodes in the filesystem that pass
	// .isOwnerOK, whether or not they're in the tree.
	nodeToRoots map[btrfsvol.LogicalAddr]rebuiltRoots
}

func (tree *RebuiltTree) acquireNodeIndex(ctx context.Context) rebuiltNodeIndex {
	return *tree.forrest.nodeIndex.Acquire(ctx, tree.ID)
}

func (tree *RebuiltTree) releaseNodeIndex() {
	tree.forrest.nodeIndex.Release(tree.ID)
}

func (tree *RebuiltTree) uncachedNodeIndex(ctx context.Context) rebuiltNodeIndex {
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.index-nodes", fmt.Sprintf("tree=%v", tree.ID))

	indexer := &rebuiltNodeIndexer{
		tree:     tree,
		idToTree: make(map[btrfsprim.ObjID]*RebuiltTree),

		nodeToRoots: make(map[btrfsvol.LogicalAddr]rebuiltRoots),
	}
	for ancestor := tree; ancestor != nil; ancestor = ancestor.Parent {
		indexer.idToTree[ancestor.ID] = ancestor
		if ancestor.ID == tree.ancestorRoot {
			break
		}
	}

	ret := rebuiltNodeIndex{
		idToTree:    indexer.idToTree,
		nodeToRoots: make(map[btrfsvol.LogicalAddr]rebuiltRoots),
	}
	for node, roots := range indexer.run(ctx) {
		if len(roots) > 0 {
			ret.nodeToRoots[node] = roots
		}
	}
	return ret
}

type rebuiltNodeIndexer struct {
	// Input
	tree     *RebuiltTree
	idToTree map[btrfsprim.ObjID]*RebuiltTree

	// Output
	nodeToRoots map[btrfsvol.LogicalAddr]rebuiltRoots

	// State
	stats          textui.Portion[int]
	progressWriter *textui.Progress[textui.Portion[int]]
}

func (indexer *rebuiltNodeIndexer) run(ctx context.Context) map[btrfsvol.LogicalAddr]rebuiltRoots {
	indexer.stats.D = len(indexer.tree.forrest.graph.Nodes)
	indexer.progressWriter = textui.NewProgress[textui.Portion[int]](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	indexer.updateProgress()
	for _, node := range maps.SortedKeys(indexer.tree.forrest.graph.Nodes) {
		indexer.node(ctx, node, nil)
	}
	indexer.progressWriter.Done()
	return indexer.nodeToRoots
}

func (indexer *rebuiltNodeIndexer) updateProgress() {
	indexer.stats.N = len(indexer.nodeToRoots)
	indexer.progressWriter.Set(indexer.stats)
}

func (indexer *rebuiltNodeIndexer) node(ctx context.Context, node btrfsvol.LogicalAddr, stack []btrfsvol.LogicalAddr) {
	defer indexer.updateProgress()
	if err := ctx.Err(); err != nil {
		return
	}
	if maps.HasKey(indexer.nodeToRoots, node) {
		return
	}
	if slices.Contains(node, stack) {
		// This is a panic because tree.forrest.graph.FinalCheck() should
		// have already checked for loops.
		panic("loop")
	}
	nodeInfo := indexer.tree.forrest.graph.Nodes[node]
	if !indexer.tree.isOwnerOK(nodeInfo.Owner, nodeInfo.Generation) {
		indexer.nodeToRoots[node] = nil
		return
	}

	stack = append(stack, node)
	var roots rebuiltRoots
nextKP:
	for _, kp := range indexer.tree.forrest.graph.EdgesTo[node] {
		// The cheap expectations to check.
		if kp.ToLevel != nodeInfo.Level ||
			kp.ToKey.Compare(nodeInfo.MinItem(indexer.tree.forrest.graph)) > 0 ||
			kp.ToGeneration != nodeInfo.Generation {
			continue nextKP
		}
		// The MaxItem expectation is only cheap to check if
		// the KP isn't in the last slot.  If it isn't in the
		// last slot, we'll wait to check it until after we've
		// indexed kp.FromNode.
		if kp.FromSlot+1 < len(indexer.tree.forrest.graph.EdgesFrom[kp.FromNode]) {
			kpMaxItem := indexer.tree.forrest.graph.EdgesFrom[kp.FromNode][kp.FromSlot+1].ToKey.Mm()
			if kpMaxItem.Compare(nodeInfo.MaxItem(indexer.tree.forrest.graph)) < 0 {
				continue nextKP
			}
		}
		// isOwnerOK is O(n) on the number of parents, so
		// avoid doing it if possible.
		if fromTree := indexer.idToTree[kp.FromTree]; fromTree == nil || !fromTree.isOwnerOK(nodeInfo.Owner, nodeInfo.Generation) {
			continue nextKP
		}

		indexer.node(ctx, kp.FromNode, stack)
		for root, rootInfo := range indexer.nodeToRoots[kp.FromNode] {
			if kp.FromSlot+1 < len(indexer.tree.forrest.graph.EdgesFrom[kp.FromNode]) {
				rootInfo.loMaxItem = indexer.tree.forrest.graph.EdgesFrom[kp.FromNode][kp.FromSlot+1].ToKey.Mm()
				rootInfo.hiMaxItem = rootInfo.loMaxItem
			} else {
				// OK, now check the MaxItem expectation.
				//
				// We'll use the hiMaxItem, because it only needs to be
				// valid in *one* of the paths to this node.
				kpMaxItem := rootInfo.hiMaxItem
				if kpMaxItem.Compare(nodeInfo.MaxItem(indexer.tree.forrest.graph)) < 0 {
					continue nextKP
				}
			}
			oldRootInfo, ok := roots[root]
			if ok && rootInfo.loMaxItem.Compare(oldRootInfo.loMaxItem) > 0 {
				rootInfo.loMaxItem = oldRootInfo.loMaxItem
			}
			if ok && rootInfo.hiMaxItem.Compare(oldRootInfo.hiMaxItem) < 0 {
				rootInfo.hiMaxItem = oldRootInfo.hiMaxItem
			}
			if roots == nil {
				roots = make(rebuiltRoots)
			}
			roots[root] = rootInfo
		}
	}
	if roots == nil {
		roots = rebuiltRoots{
			node: rebuiltPathInfo{
				loMaxItem: btrfsprim.MaxKey,
				hiMaxItem: btrfsprim.MaxKey,
			},
		}
	}
	indexer.nodeToRoots[node] = roots
}

// isOwnerOK returns whether it is permissible for a node with
// .Head.Owner=owner and .Head.Generation=gen to be in this tree.
func (tree *RebuiltTree) isOwnerOK(owner btrfsprim.ObjID, gen btrfsprim.Generation) bool {
	// It is important that this not perform allocations, even in
	// the "false"/failure case.  It will be called lots of times
	// in a tight loop for both values that pass and values that
	// fail.
	root := tree.ancestorRoot
	for {
		if owner == tree.ID {
			return true
		}
		if tree.Parent == nil || gen > tree.ParentGen || tree.ID == root {
			return false
		}
		tree = tree.Parent
	}
}

// evictable members 2 and 3: .Rebuilt{Acquire,Release}{Potential,}Items() /////////////////////////////////////////////

// RebuiltAcquireItems returns a map of the items contained in this
// tree.
//
// Do not mutate the returned map; it is a pointer to the
// RebuiltTree's internal map!
//
// When done with the map, call .RebuiltReleaseItems().
func (tree *RebuiltTree) RebuiltAcquireItems(ctx context.Context) *containers.SortedMap[btrfsprim.Key, ItemPtr] {
	tree.forrest.commitTrees(ctx, tree.ID)
	tree.initRoots(ctx)
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	return tree.forrest.incItems.Acquire(ctx, tree.ID)
}

// RebuiltReleaseItems releases resources after a call to
// .RebuiltAcquireItems().
func (tree *RebuiltTree) RebuiltReleaseItems() {
	tree.forrest.incItems.Release(tree.ID)
}

// RebuiltAcquirePotentialItems returns a map of items that could be
// added to this tree with .RebuiltAddRoot().
//
// Do not mutate the returned map; it is a pointer to the
// RebuiltTree's internal map!
//
// When done with the map, call .RebuiltReleasePotentialItems().
func (tree *RebuiltTree) RebuiltAcquirePotentialItems(ctx context.Context) *containers.SortedMap[btrfsprim.Key, ItemPtr] {
	tree.forrest.commitTrees(ctx, tree.ID)
	tree.initRoots(ctx)
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	return tree.forrest.excItems.Acquire(ctx, tree.ID)
}

// RebuiltReleasePotentialItems releases resources after a call to
// .RebuiltAcquirePotentialItems().
func (tree *RebuiltTree) RebuiltReleasePotentialItems() {
	tree.forrest.excItems.Release(tree.ID)
}

func (tree *RebuiltTree) uncachedIncItems(ctx context.Context) containers.SortedMap[btrfsprim.Key, ItemPtr] {
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.index-inc-items", fmt.Sprintf("tree=%v", tree.ID))
	return tree.uncachedItems(ctx, true)
}

func (tree *RebuiltTree) uncachedExcItems(ctx context.Context) containers.SortedMap[btrfsprim.Key, ItemPtr] {
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.index-exc-items", fmt.Sprintf("tree=%v", tree.ID))
	return tree.uncachedItems(ctx, false)
}

type rebuiltItemStats struct {
	Leafs    textui.Portion[int]
	NumItems int
	NumDups  int
}

func (s rebuiltItemStats) String() string {
	return textui.Sprintf("%v (%v items, %v dups)",
		s.Leafs, s.NumItems, s.NumDups)
}

func (tree *RebuiltTree) uncachedItems(ctx context.Context, inc bool) containers.SortedMap[btrfsprim.Key, ItemPtr] {
	var leafs []btrfsvol.LogicalAddr
	for node, roots := range tree.acquireNodeIndex(ctx).nodeToRoots {
		if tree.forrest.graph.Nodes[node].Level == 0 && maps.HaveAnyKeysInCommon(tree.Roots, roots) == inc {
			leafs = append(leafs, node)
		}
	}
	tree.releaseNodeIndex()
	slices.Sort(leafs)

	var stats rebuiltItemStats
	stats.Leafs.D = len(leafs)
	progressWriter := textui.NewProgress[rebuiltItemStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))

	var index containers.SortedMap[btrfsprim.Key, ItemPtr]
	for i, leaf := range leafs {
		stats.Leafs.N = i
		progressWriter.Set(stats)
		for j, itemKeyAndSize := range tree.forrest.graph.Nodes[leaf].Items {
			newPtr := ItemPtr{
				Node: leaf,
				Slot: j,
			}
			if oldPtr, exists := index.Load(itemKeyAndSize.Key); !exists {
				index.Store(itemKeyAndSize.Key, newPtr)
				stats.NumItems++
			} else {
				if tree.RebuiltShouldReplace(oldPtr.Node, newPtr.Node) {
					index.Store(itemKeyAndSize.Key, newPtr)
				}
				stats.NumDups++
			}
			progressWriter.Set(stats)
		}
	}
	stats.Leafs.N = stats.Leafs.D
	progressWriter.Set(stats)
	progressWriter.Done()

	return index
}

// evictable member 4: .addErrs() //////////////////////////////////////////////////////////////////////////////////////

type rebuiltTreeError struct {
	Min  btrfsprim.Key
	Max  btrfsprim.Key
	Node btrfsvol.LogicalAddr
	Err  error
}

func (e rebuiltTreeError) Error() string {
	return fmt.Sprintf("keys %v-%v: node@%v: %v", e.Min, e.Max, e.Node, e.Err)
}

func (e rebuiltTreeError) Unwrap() error {
	return e.Err
}

type errorStats struct {
	Nodes   textui.Portion[int]
	NumErrs int
}

func (s errorStats) String() string {
	return textui.Sprintf("%v (%v errs)",
		s.Nodes, s.NumErrs)
}

func (tree *RebuiltTree) uncachedErrors(ctx context.Context) containers.IntervalTree[btrfsprim.Key, rebuiltTreeError] {
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.index-errors", fmt.Sprintf("tree=%v", tree.ID))

	tree.mu.RLock()
	defer tree.mu.RUnlock()

	errs := containers.IntervalTree[btrfsprim.Key, rebuiltTreeError]{
		MinFn: func(err rebuiltTreeError) btrfsprim.Key {
			return err.Min
		},
		MaxFn: func(err rebuiltTreeError) btrfsprim.Key {
			return err.Max
		},
	}

	nodeIndex := tree.acquireNodeIndex(ctx)
	defer tree.releaseNodeIndex()

	nodesToProcess := make(containers.Set[btrfsvol.LogicalAddr], len(nodeIndex.nodeToRoots))
	for node := range nodeIndex.nodeToRoots {
		if !maps.HaveAnyKeysInCommon(nodeIndex.nodeToRoots[node], tree.Roots) {
			continue
		}
		nodesToProcess.Insert(node)
		for _, kp := range tree.forrest.graph.EdgesFrom[node] {
			nodesToProcess.Insert(kp.ToNode)
		}
	}
	for root := range tree.Roots {
		// For BadNodes that are roots.
		nodesToProcess.Insert(root)
	}

	var stats errorStats
	stats.Nodes.D = len(nodesToProcess)
	progressWriter := textui.NewProgress[errorStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	progressWriter.Set(stats)

	var kps []*GraphEdge
	for i, node := range maps.SortedKeys(nodesToProcess) {
		stats.Nodes.N = i
		progressWriter.Set(stats)

		// `node` may either be in the tree, or just pointed
		// to by a different node in the tree.  Decide whether
		// it's in the tree, and gather all of the
		// key-pointers in the tree that point to it.
		inTree := maps.HaveAnyKeysInCommon(nodeIndex.nodeToRoots[node], tree.Roots)
		kps = kps[:0]
		for _, kp := range tree.forrest.graph.EdgesTo[node] {
			if !maps.HaveAnyKeysInCommon(nodeIndex.nodeToRoots[kp.FromNode], tree.Roots) {
				continue
			}
			kps = append(kps, kp)
		}

		// Look at all key-pointers to decide what our
		// expectations are.
		var (
			expLevel  = make(containers.Set[uint8], len(kps))
			expGen    = make(containers.Set[btrfsprim.Generation], len(kps))
			expTree   = make(containers.Set[btrfsprim.ObjID], len(kps))
			loMinItem = btrfsprim.MaxKey // lowest kp.ToKey seen
			hiMinItem = btrfsprim.Key{}  // highest kp.ToKey seen
			loMaxItem = btrfsprim.MaxKey // lowest nodeToRoots[node][root] seen
			hiMaxItem = btrfsprim.Key{}  // highest nodeToRoots[node][root] seen
		)
		expTree.Insert(tree.ID)
		if len(kps) == 0 {
			// This is a root.
			loMinItem = btrfsprim.Key{}
			hiMaxItem = btrfsprim.MaxKey
		} else {
			// expLevel, expGen, loMinItem, hiMinItem,
			for _, kp := range kps {
				expLevel.Insert(kp.ToLevel)
				expGen.Insert(kp.ToGeneration)
				expTree.Insert(kp.FromTree)
				if kp.ToKey.Compare(loMinItem) < 0 {
					loMinItem = kp.ToKey
				}
				if kp.ToKey.Compare(hiMinItem) > 0 {
					hiMinItem = kp.ToKey
				}
			}
			// loMaxItem, hiMaxItem
			if !inTree {
				for _, kp := range kps {
					for root, rootInfo := range nodeIndex.nodeToRoots[kp.FromNode] {
						if !tree.Roots.Has(root) {
							continue
						}
						if kp.FromSlot+1 < len(tree.forrest.graph.EdgesFrom[kp.FromNode]) {
							rootInfo.loMaxItem = tree.forrest.graph.EdgesFrom[kp.FromNode][kp.FromSlot+1].ToKey.Mm()
							rootInfo.hiMaxItem = rootInfo.loMaxItem
						}
						if loMaxItem.Compare(rootInfo.loMaxItem) > 0 {
							loMaxItem = rootInfo.loMaxItem
						}
						if hiMaxItem.Compare(rootInfo.hiMaxItem) < 0 {
							hiMaxItem = rootInfo.hiMaxItem
						}
					}
				}
			} else {
				// As an optimization, we can look at this node's rootInfo directly.
				// This should be equivalent to the above loop for `!inTree`, but is
				// faster.
				for root, rootInfo := range nodeIndex.nodeToRoots[node] {
					if !tree.Roots.Has(root) {
						continue
					}
					if loMaxItem.Compare(rootInfo.loMaxItem) > 0 {
						loMaxItem = rootInfo.loMaxItem
					}
					if hiMaxItem.Compare(rootInfo.hiMaxItem) < 0 {
						hiMaxItem = rootInfo.hiMaxItem
					}
				}
			}
		}

		// Assemble all of that in to a btrfstree.NodeExpectations.
		var nodeErrs derror.MultiError
		exp := btrfstree.NodeExpectations{
			LAddr:   containers.OptionalValue(node),
			MinItem: containers.OptionalValue(hiMinItem),
			MaxItem: containers.OptionalValue(loMaxItem),
			Owner: func(owner btrfsprim.ObjID, gen btrfsprim.Generation) error {
				byStr := make(map[string]error)
				for _, kpTree := range maps.SortedKeys(expTree) {
					if err := btrfstree.CheckOwner(ctx, tree.forrest, kpTree, owner, gen); err != nil {
						byStr[err.Error()] = err
					}
				}
				if len(byStr) > 0 {
					byPos := make(derror.MultiError, 0, len(byStr))
					for _, str := range maps.SortedKeys(byStr) {
						byPos = append(byPos, byStr[str])
					}
					return byPos
				}
				return nil
			},
		}
		switch len(expLevel) {
		case 0:
			// do nothing
		case 1:
			exp.Level = containers.OptionalValue(expLevel.TakeOne())
		default:
			nodeErrs = append(nodeErrs,
				fmt.Errorf("multiple KPs request different node levels: %v (actual: %v)",
					maps.SortedKeys(expLevel), tree.forrest.graph.Nodes[node].Level))
		}
		switch len(expGen) {
		case 0:
			// do nothing
		case 1:
			exp.Generation = containers.OptionalValue(expGen.TakeOne())
		default:
			nodeErrs = append(nodeErrs,
				fmt.Errorf("multiple KPs request different node generations: %v (actual: %v)",
					maps.SortedKeys(expGen), tree.forrest.graph.Nodes[node].Generation))
		}

		// Check those expectations.
		if hiMaxItem.Compare(loMinItem) < 0 {
			nodeErrs = append(nodeErrs,
				fmt.Errorf("loMinItem:%v > hiMaxItem:%v", loMinItem, hiMaxItem))
			loMinItem = btrfsprim.Key{}
			hiMaxItem = btrfsprim.MaxKey
		}
		if err := tree.forrest.graph.BadNodes[node]; err != nil {
			nodeErrs = append(nodeErrs, err)
		} else if err := tree.forrest.graph.Nodes[node].CheckExpectations(tree.forrest.graph, exp); err != nil {
			nodeErrs = append(nodeErrs, err)
		}

		if len(nodeErrs) > 0 {
			errs.Insert(rebuiltTreeError{
				Min:  loMinItem,
				Max:  hiMaxItem,
				Node: node,
				Err:  nodeErrs,
			})

			stats.NumErrs++
			progressWriter.Set(stats)
		}
	}
	stats.Nodes.N = stats.Nodes.D
	progressWriter.Set(stats)
	progressWriter.Done()

	return errs
}

func (tree *RebuiltTree) addErrs(ctx context.Context, fn func(btrfsprim.Key, uint32) int, err error) error {
	var errs derror.MultiError
	tree.forrest.errors.Acquire(ctx, tree.ID).Subrange(
		func(k btrfsprim.Key) int { return fn(k, 0) },
		func(v rebuiltTreeError) bool {
			errs = append(errs, v)
			return true
		})
	tree.forrest.errors.Release(tree.ID)
	if len(errs) == 0 {
		return err
	}
	if err != nil {
		errs = append(errs, err)
	}
	return errs
}

// main public API /////////////////////////////////////////////////////////////////////////////////////////////////////

func (tree *RebuiltTree) RebuiltShouldReplace(oldNode, newNode btrfsvol.LogicalAddr) bool {
	oldDist, _ := tree.RebuiltCOWDistance(tree.forrest.graph.Nodes[oldNode].Owner)
	newDist, _ := tree.RebuiltCOWDistance(tree.forrest.graph.Nodes[newNode].Owner)
	switch {
	case newDist < oldDist:
		// Replace the old one with the new lower-dist one.
		return true
	case newDist > oldDist:
		// Retain the old lower-dist one.
		return false
	default:
		oldGen := tree.forrest.graph.Nodes[oldNode].Generation
		newGen := tree.forrest.graph.Nodes[newNode].Generation
		switch {
		case newGen > oldGen:
			// Replace the old one with the new higher-gen one.
			return true
		case newGen < oldGen:
			// Retain the old higher-gen one.
			return false
		default:
			// TODO: This is a panic because I'm not really sure what the
			// best way to handle this is, and so if this happens I want the
			// program to crash and force me to figure out how to handle it.
			panic(fmt.Errorf("dup nodes in tree=%v: old=%v=%v ; new=%v=%v",
				tree.ID,
				oldNode, tree.forrest.graph.Nodes[oldNode],
				newNode, tree.forrest.graph.Nodes[newNode]))
		}
	}
}

type rebuiltRootStats struct {
	Nodes      textui.Portion[int]
	AddedLeafs int
	AddedItems int
}

func (s rebuiltRootStats) String() string {
	return textui.Sprintf("%v (added %v leafs, added %v items)",
		s.Nodes, s.AddedLeafs, s.AddedItems)
}

// RebuiltAddRoot adds an additional root node to the tree.  It is
// useful to call .RebuiltAddRoot() to re-attach part of the tree that
// has been broken off.
//
// If the RebuiltForrest has laxAncestors=false, then:
//
//   - calls to RebuiltForrestExtendedCallbacks.AddedItem() are
//     inhibited.
//
//   - calling RebuiltAddRoot on the ROOT_TREE or the UUID_TREE will
//     panic if a tree other than the ROOT_TREE or UUID_TREE has been
//     read from.
func (tree *RebuiltTree) RebuiltAddRoot(ctx context.Context, rootNode btrfsvol.LogicalAddr) {
	tree.mu.Lock()
	defer tree.mu.Unlock()

	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.add-root", fmt.Sprintf("tree=%v rootNode=%v", tree.ID, rootNode))
	dlog.Info(ctx, "adding root...")

	shouldFlush := tree.ID == btrfsprim.ROOT_TREE_OBJECTID || tree.ID == btrfsprim.UUID_TREE_OBJECTID

	if tree.forrest.laxAncestors && shouldFlush {
		_ = tree.forrest.treesMu.Lock(ctx)
		if tree.forrest.treesCommitted {
			panic(fmt.Errorf("RebuiltTree(%v).RebuiltAddRoot called after a non-ROOT, non-UUID tree (%v) has been read from",
				tree.ID, tree.forrest.treesCommitter))
		}
		tree.forrest.treesMu.Unlock()
	}

	if extCB, ok := tree.forrest.cb.(RebuiltForrestExtendedCallbacks); ok && !tree.forrest.laxAncestors {
		var stats rebuiltRootStats
		nodeToRoots := tree.acquireNodeIndex(ctx).nodeToRoots
		stats.Nodes.D = len(nodeToRoots)
		progressWriter := textui.NewProgress[rebuiltRootStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
		for i, node := range maps.SortedKeys(nodeToRoots) {
			stats.Nodes.N = i
			progressWriter.Set(stats)

			if tree.forrest.graph.Nodes[node].Level > 0 || maps.HaveAnyKeysInCommon(tree.Roots, nodeToRoots[node]) || !maps.HasKey(nodeToRoots[node], rootNode) {
				continue
			}

			stats.AddedLeafs++
			progressWriter.Set(stats)

			for _, itemKeyAndSize := range tree.forrest.graph.Nodes[node].Items {
				extCB.AddedItem(ctx, tree.ID, itemKeyAndSize.Key)
				stats.AddedItems++
				progressWriter.Set(stats)
			}
		}
		stats.Nodes.N = len(nodeToRoots)
		tree.releaseNodeIndex()
		progressWriter.Set(stats)
		progressWriter.Done()

		if stats.AddedItems == 0 {
			shouldFlush = false
		}
	}

	tree.Roots.Insert(rootNode)
	tree.forrest.incItems.Delete(tree.ID) // force re-gen
	tree.forrest.excItems.Delete(tree.ID) // force re-gen
	tree.forrest.errors.Delete(tree.ID)   // force re-gen

	if shouldFlush {
		tree.forrest.flushNegativeCache(ctx)
	}
	tree.forrest.cb.AddedRoot(ctx, tree.ID, rootNode)
}

// RebuiltCOWDistance returns how many COW-snapshots down the 'tree'
// is from the 'parent'.
func (tree *RebuiltTree) RebuiltCOWDistance(parentID btrfsprim.ObjID) (dist int, ok bool) {
	root := tree.ancestorRoot
	for {
		if parentID == tree.ID {
			return dist, true
		}
		if tree.Parent == nil || tree.ID == root {
			return 0, false
		}
		tree = tree.Parent
		dist++
	}
}

// RebuiltLeafToRoots returns the list of potential roots (to pass to
// .RebuiltAddRoot) that include a given leaf-node.
func (tree *RebuiltTree) RebuiltLeafToRoots(ctx context.Context, leaf btrfsvol.LogicalAddr) containers.Set[btrfsvol.LogicalAddr] {
	if tree.forrest.graph.Nodes[leaf].Level != 0 {
		panic(fmt.Errorf("should not happen: (tree=%v).RebuiltLeafToRoots(leaf=%v): not a leaf",
			tree.ID, leaf))
	}

	tree.forrest.commitTrees(ctx, tree.ID)
	tree.initRoots(ctx)
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	ret := make(containers.Set[btrfsvol.LogicalAddr])
	for root := range tree.acquireNodeIndex(ctx).nodeToRoots[leaf] {
		if tree.Roots.Has(root) {
			panic(fmt.Errorf("should not happen: (tree=%v).RebuiltLeafToRoots(leaf=%v): tree contains root=%v but not leaf",
				tree.ID, leaf, root))
		}
		ret.Insert(root)
	}
	tree.releaseNodeIndex()
	if len(ret) == 0 {
		return nil
	}
	return ret
}

// btrfstree.Tree interface ////////////////////////////////////////////////////////////////////////////////////////////

var _ btrfstree.Tree = (*RebuiltTree)(nil)

// TreeParentID implements btrfstree.Tree.
func (tree *RebuiltTree) TreeParentID(_ context.Context) (btrfsprim.ObjID, btrfsprim.Generation, error) {
	switch {
	case tree.parentErr != nil:
		return 0, 0, tree.parentErr
	case tree.Parent == nil:
		return 0, 0, nil
	default:
		return tree.Parent.ID, tree.ParentGen, nil
	}
}

// TreeLookup implements btrfstree.Tree.
func (tree *RebuiltTree) TreeLookup(ctx context.Context, key btrfsprim.Key) (btrfstree.Item, error) {
	return tree.TreeSearch(ctx, btrfstree.SearchExactKey(key))
}

// TreeSearch implements btrfstree.Tree.  It is a thin wrapper around
// tree.RebuiltItems(ctx).Search (to do the search) and
// tree.TreeLookup (to read item bodies).
func (tree *RebuiltTree) TreeSearch(ctx context.Context, searcher btrfstree.TreeSearcher) (btrfstree.Item, error) {
	tree.forrest.commitTrees(ctx, tree.ID)
	tree.initRoots(ctx)
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	_, ptr, ok := tree.RebuiltAcquireItems(ctx).Search(func(_ btrfsprim.Key, ptr ItemPtr) int {
		straw := tree.forrest.graph.Nodes[ptr.Node].Items[ptr.Slot]
		return searcher.Search(straw.Key, straw.Size)
	})
	tree.RebuiltReleaseItems()
	if !ok {
		return btrfstree.Item{}, fmt.Errorf("item with %s: %w", searcher, tree.addErrs(ctx, searcher.Search, btrfstree.ErrNoItem))
	}
	return tree.forrest.readItem(ctx, ptr), nil
}

// TreeRange implements btrfstree.Tree.  It is a thin wrapper around
// tree.RebuiltItems(ctx).Range (to do the iteration) and
// tree.TreeLookup (to read item bodies).
func (tree *RebuiltTree) TreeRange(ctx context.Context, handleFn func(btrfstree.Item) bool) error {
	tree.forrest.commitTrees(ctx, tree.ID)
	tree.initRoots(ctx)
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	tree.RebuiltAcquireItems(ctx).Range(func(_ btrfsprim.Key, ptr ItemPtr) bool {
		return handleFn(tree.forrest.readItem(ctx, ptr))
	})
	tree.RebuiltReleaseItems()
	return tree.addErrs(ctx, func(btrfsprim.Key, uint32) int { return 0 }, nil)
}

// TreeSubrange implements btrfstree.Tree.  It is a thin wrapper
// around tree.RebuiltItems(ctx).Subrange (to do the iteration) and
// tree.TreeLookup (to read item bodies).
func (tree *RebuiltTree) TreeSubrange(ctx context.Context,
	min int,
	searcher btrfstree.TreeSearcher,
	handleFn func(btrfstree.Item) bool,
) error {
	tree.forrest.commitTrees(ctx, tree.ID)
	tree.initRoots(ctx)
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	var cnt int
	tree.RebuiltAcquireItems(ctx).Subrange(
		func(_ btrfsprim.Key, ptr ItemPtr) int {
			straw := tree.forrest.graph.Nodes[ptr.Node].Items[ptr.Slot]
			return searcher.Search(straw.Key, straw.Size)
		},
		func(_ btrfsprim.Key, ptr ItemPtr) bool {
			cnt++
			return handleFn(tree.forrest.readItem(ctx, ptr))
		},
	)
	tree.RebuiltReleaseItems()

	var err error
	if cnt < min {
		err = btrfstree.ErrNoItem
	}
	err = tree.addErrs(ctx, searcher.Search, err)
	if err != nil {
		return fmt.Errorf("items with %s: %w", searcher, err)
	}

	return nil
}

// TreeWalk implements btrfstree.Tree.
func (tree *RebuiltTree) TreeWalk(ctx context.Context, cbs btrfstree.TreeWalkHandler) {
	tree.forrest.commitTrees(ctx, tree.ID)
	tree.initRoots(ctx)
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	if _, err := tree.forrest.Superblock(); err != nil && cbs.BadSuperblock != nil {
		cbs.BadSuperblock(err)
	}

	walker := &rebuiltWalker{
		// Input: tree
		tree:      tree,
		nodeIndex: tree.acquireNodeIndex(ctx),
		items:     tree.RebuiltAcquireItems(ctx),

		// Input: args
		cbs: cbs,

		// State
		visited: make(containers.Set[btrfsvol.LogicalAddr]),
	}
	defer tree.releaseNodeIndex()
	defer tree.RebuiltReleaseItems()

	for _, root := range maps.SortedKeys(tree.Roots) {
		path := btrfstree.Path{
			btrfstree.PathRoot{
				Forrest:      tree.forrest,
				TreeID:       tree.ID,
				ToAddr:       root,
				ToGeneration: tree.forrest.graph.Nodes[root].Generation,
				ToLevel:      tree.forrest.graph.Nodes[root].Level,
			},
		}
		walker.walk(ctx, path)
		if ctx.Err() != nil {
			return
		}
	}
}

type rebuiltWalker struct {
	// Input: tree
	tree      *RebuiltTree
	nodeIndex rebuiltNodeIndex
	items     *containers.SortedMap[btrfsprim.Key, ItemPtr]

	// Input: args
	cbs btrfstree.TreeWalkHandler

	// State
	visited containers.Set[btrfsvol.LogicalAddr]
}

func (walker *rebuiltWalker) walk(ctx context.Context, path btrfstree.Path) {
	if ctx.Err() != nil {
		return
	}

	// 001
	nodeAddr, nodeExp, ok := path.NodeExpectations(ctx)
	if !ok {
		panic(fmt.Errorf("should not happen: btrfsutil.rebuiltWalker.walk called with non-node path: %v",
			path))
	}
	if err := walker.tree.forrest.graph.BadNodes[nodeAddr]; err != nil {
		if walker.cbs.BadNode != nil {
			_ = walker.cbs.BadNode(path, nil, err)
		}
		return
	}
	// 001-002
	nodeInfo := walker.tree.forrest.graph.Nodes[nodeAddr]
	if err := nodeInfo.CheckExpectations(walker.tree.forrest.graph, nodeExp); err != nil {
		if walker.cbs.BadNode != nil {
			// 001
			node, _ := walker.tree.forrest.AcquireNode(ctx, nodeAddr, nodeExp)
			defer walker.tree.forrest.ReleaseNode(node)
			if ctx.Err() != nil {
				return
			}
			// 002
			_ = walker.cbs.BadNode(path, node, err)
		}
		return
	}
	if !maps.HaveAnyKeysInCommon(walker.tree.Roots, walker.nodeIndex.nodeToRoots[nodeAddr]) {
		panic(fmt.Errorf("should not happen: node@%v is not in the tree, but did not error: path=%#v exp=%#v",
			nodeAddr, path, nodeExp))
	}
	if walker.visited.Has(nodeAddr) {
		return
	}
	walker.visited.Insert(nodeAddr)
	if walker.cbs.Node != nil {
		// 001
		node, _ := walker.tree.forrest.AcquireNode(ctx, nodeAddr, nodeExp)
		if ctx.Err() != nil {
			walker.tree.forrest.ReleaseNode(node)
			return
		}
		// 002
		walker.cbs.Node(path, node)
		walker.tree.forrest.ReleaseNode(node)
		if ctx.Err() != nil {
			return
		}
	}

	// branch a (interior)
	for i, kp := range walker.tree.forrest.graph.EdgesFrom[nodeAddr] {
		var toMaxKey btrfsprim.Key
		for root, rootInfo := range walker.nodeIndex.nodeToRoots[nodeAddr] {
			if !walker.tree.Roots.Has(root) {
				continue
			}
			if rootInfo.hiMaxItem.Compare(toMaxKey) > 0 {
				toMaxKey = rootInfo.hiMaxItem
			}
		}
		itemPath := append(path, btrfstree.PathKP{
			FromTree: walker.tree.forrest.graph.Nodes[nodeAddr].Owner,
			FromSlot: i,

			ToAddr:       kp.ToNode,
			ToGeneration: kp.ToGeneration,
			ToMinKey:     kp.ToKey,

			ToMaxKey: toMaxKey,
			ToLevel:  kp.ToLevel,
		})
		// 003a
		recurse := walker.cbs.KeyPointer == nil || walker.cbs.KeyPointer(itemPath, btrfstree.KeyPointer{
			Key:        kp.ToKey,
			BlockPtr:   kp.ToNode,
			Generation: kp.ToGeneration,
		})
		if ctx.Err() != nil {
			return
		}
		// 004a
		if recurse {
			walker.walk(ctx, itemPath)
			if ctx.Err() != nil {
				return
			}
		}
	}

	// branch b (leaf)
	if walker.cbs.Item != nil || walker.cbs.BadItem != nil {
		for i, keyAndSize := range walker.tree.forrest.graph.Nodes[nodeAddr].Items {
			ptr, ok := walker.items.Load(keyAndSize.Key)
			if !ok {
				panic(fmt.Errorf("should not happen: index does not contain present item %v", keyAndSize.Key))
			}
			if ptr.Node != nodeAddr {
				continue
			}
			itemPath := append(path, btrfstree.PathItem{
				FromTree: walker.tree.forrest.graph.Nodes[nodeAddr].Owner,
				FromSlot: i,

				ToKey: keyAndSize.Key,
			})
			item := walker.tree.forrest.readItem(ctx, ptr)
			// 003b
			switch item.Body.(type) {
			case *btrfsitem.Error:
				if walker.cbs.BadItem != nil {
					walker.cbs.BadItem(itemPath, item)
				}
			default:
				if walker.cbs.Item != nil {
					walker.cbs.Item(itemPath, item)
				}
			}
			if ctx.Err() != nil {
				return
			}
		}
	}
}
