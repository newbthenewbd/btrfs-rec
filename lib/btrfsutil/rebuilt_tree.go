// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type RebuiltTree struct {
	// static
	ID        btrfsprim.ObjID
	UUID      btrfsprim.UUID
	Parent    *RebuiltTree
	ParentGen btrfsprim.Generation // offset of this tree's root item
	forrest   *RebuiltForrest

	// mutable
	mu    sync.RWMutex
	Roots containers.Set[btrfsvol.LogicalAddr]
	// There are 3 more mutable "members" that are protected by
	// `mu`; but they live in a shared Cache.  They are all
	// derived from tree.Roots, which is why it's OK if they get
	// evicted.
	//
	//  1. tree.acquireNodeIndex()             = tree.forrest.nodeIndex.Acquire(tree.ID)
	//  2. tree.RebuiltAcquireItems()          = tree.forrest.incItems.Acquire(tree.ID)
	//  3. tree.RebuiltAcquirePotentialItems() = tree.forrest.excItems.Acquire(tree.ID)
}

type rebuiltSharedCache struct {
	nodeIndex containers.Cache[btrfsprim.ObjID, rebuiltNodeIndex]
	incItems  containers.Cache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]]
	excItems  containers.Cache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]]
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
	return ret
}

// evictable member 1: .acquireNodeIndex() /////////////////////////////////////////////////////////////////////////////

type rebuiltRoots = map[btrfsvol.LogicalAddr]rebuiltPathInfo

type rebuiltPathInfo struct {
	loMaxItem btrfsprim.Key
	hiMaxItem btrfsprim.Key
}

type rebuiltNodeIndex struct {
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
	}

	ret := rebuiltNodeIndex{
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
		if len(indexer.nodeToRoots[kp.FromNode]) > 0 {
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
	for {
		if owner == tree.ID {
			return true
		}
		if tree.Parent == nil || gen > tree.ParentGen {
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
	return tree.forrest.excItems.Acquire(ctx, tree.ID)
}

// RebuiltReleasePotentialItems releases resources after a call to
// .RebuiltAcquirePotentialItems().
func (tree *RebuiltTree) RebuiltReleasePotentialItems() {
	tree.forrest.excItems.Release(tree.ID)
}

func (tree *RebuiltTree) uncachedIncItems(ctx context.Context) containers.SortedMap[btrfsprim.Key, ItemPtr] {
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.index-inc-items", fmt.Sprintf("tree=%v", tree.ID))
	return tree.items(ctx, true)
}

func (tree *RebuiltTree) uncachedExcItems(ctx context.Context) containers.SortedMap[btrfsprim.Key, ItemPtr] {
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.index-exc-items", fmt.Sprintf("tree=%v", tree.ID))
	return tree.items(ctx, false)
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

func (tree *RebuiltTree) items(ctx context.Context, inc bool) containers.SortedMap[btrfsprim.Key, ItemPtr] {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

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
func (tree *RebuiltTree) RebuiltAddRoot(ctx context.Context, rootNode btrfsvol.LogicalAddr) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.add-root", fmt.Sprintf("tree=%v rootNode=%v", tree.ID, rootNode))
	dlog.Info(ctx, "adding root...")

	shouldFlush := tree.ID == btrfsprim.ROOT_TREE_OBJECTID || tree.ID == btrfsprim.UUID_TREE_OBJECTID

	if extCB, ok := tree.forrest.cb.(RebuiltForrestExtendedCallbacks); ok {
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

	if shouldFlush {
		tree.forrest.flushNegativeCache(ctx)
	}
	tree.forrest.cb.AddedRoot(ctx, tree.ID, rootNode)
}

// RebuiltCOWDistance returns how many COW-snapshots down the 'tree'
// is from the 'parent'.
func (tree *RebuiltTree) RebuiltCOWDistance(parentID btrfsprim.ObjID) (dist int, ok bool) {
	for {
		if parentID == tree.ID {
			return dist, true
		}
		if tree.Parent == nil {
			return 0, false
		}
		tree = tree.Parent
		dist++
	}
}

// ReadItem reads an item from a tree.
func (tree *RebuiltTree) ReadItem(ctx context.Context, key btrfsprim.Key) btrfsitem.Item {
	ptr, ok := tree.RebuiltAcquireItems(ctx).Load(key)
	tree.RebuiltReleaseItems()
	if !ok {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltTree.ReadItem called for not-included key: %v", key))
	}
	return tree.forrest.readItem(ctx, ptr)
}

// RebuiltLeafToRoots returns the list of potential roots (to pass to
// .RebuiltAddRoot) that include a given leaf-node.
func (tree *RebuiltTree) RebuiltLeafToRoots(ctx context.Context, leaf btrfsvol.LogicalAddr) containers.Set[btrfsvol.LogicalAddr] {
	if tree.forrest.graph.Nodes[leaf].Level != 0 {
		panic(fmt.Errorf("should not happen: (tree=%v).RebuiltLeafToRoots(leaf=%v): not a leaf",
			tree.ID, leaf))
	}
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
