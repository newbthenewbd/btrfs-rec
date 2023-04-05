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
	//  1. tree.acquireLeafToRoots()           = tree.forrest.leafs.Acquire(tree.ID)
	//  2. tree.RebuiltAcquireItems()          = tree.forrest.incItems.Acquire(tree.ID)
	//  3. tree.RebuiltAcquirePotentialItems() = tree.forrest.excItems.Acquire(tree.ID)
}

type rebuiltSharedCache struct {
	leafs    containers.Cache[btrfsprim.ObjID, map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]]
	incItems containers.Cache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]]
	excItems containers.Cache[btrfsprim.ObjID, containers.SortedMap[btrfsprim.Key, ItemPtr]]
}

func makeRebuiltSharedCache(forrest *RebuiltForrest) rebuiltSharedCache {
	var ret rebuiltSharedCache
	ret.leafs = containers.NewARCache[btrfsprim.ObjID, map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]](
		textui.Tunable(8),
		containers.SourceFunc[btrfsprim.ObjID, map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]](
			func(ctx context.Context, treeID btrfsprim.ObjID, leafs *map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]) {
				*leafs = forrest.trees[treeID].uncachedLeafToRoots(ctx)
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

// evictable member 1: .acquireLeafToRoots() ///////////////////////////////////////////////////////////////////////////

// acquireLeafToRoots returns all leafs (lvl=0) in the filesystem that
// pass .isOwnerOK, whether or not they're in the tree.
func (tree *RebuiltTree) acquireLeafToRoots(ctx context.Context) map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr] {
	return *tree.forrest.leafs.Acquire(ctx, tree.ID)
}

func (tree *RebuiltTree) releaseLeafToRoots() {
	tree.forrest.leafs.Release(tree.ID)
}

func (tree *RebuiltTree) uncachedLeafToRoots(ctx context.Context) map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr] {
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.index-nodes", fmt.Sprintf("tree=%v", tree.ID))

	nodeToRoots := make(map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr])

	var stats textui.Portion[int]
	stats.D = len(tree.forrest.graph.Nodes)
	progressWriter := textui.NewProgress[textui.Portion[int]](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	progress := func() {
		stats.N = len(nodeToRoots)
		progressWriter.Set(stats)
	}

	progress()
	for _, node := range maps.SortedKeys(tree.forrest.graph.Nodes) {
		tree.indexNode(ctx, node, nodeToRoots, progress, nil)
	}
	progressWriter.Done()

	ret := make(map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr])
	for node, roots := range nodeToRoots {
		if tree.forrest.graph.Nodes[node].Level == 0 && len(roots) > 0 {
			ret[node] = roots
		}
	}
	return ret
}

func (tree *RebuiltTree) indexNode(ctx context.Context, node btrfsvol.LogicalAddr, index map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr], progress func(), stack []btrfsvol.LogicalAddr) {
	defer progress()
	if err := ctx.Err(); err != nil {
		return
	}
	if maps.HasKey(index, node) {
		return
	}
	if slices.Contains(node, stack) {
		// This is a panic because tree.forrest.graph.FinalCheck() should
		// have already checked for loops.
		panic("loop")
	}
	if !tree.isOwnerOK(tree.forrest.graph.Nodes[node].Owner, tree.forrest.graph.Nodes[node].Generation) {
		index[node] = nil
		return
	}

	// tree.leafToRoots
	stack = append(stack, node)
	var roots containers.Set[btrfsvol.LogicalAddr]
	for _, kp := range tree.forrest.graph.EdgesTo[node] {
		if !tree.isOwnerOK(tree.forrest.graph.Nodes[kp.FromNode].Owner, tree.forrest.graph.Nodes[kp.FromNode].Generation) {
			continue
		}
		tree.indexNode(ctx, kp.FromNode, index, progress, stack)
		if len(index[kp.FromNode]) > 0 {
			if roots == nil {
				roots = make(containers.Set[btrfsvol.LogicalAddr])
			}
			roots.InsertFrom(index[kp.FromNode])
		}
	}
	if roots == nil {
		roots = containers.NewSet[btrfsvol.LogicalAddr](node)
	}
	index[node] = roots
}

// isOwnerOK returns whether it is permissible for a node with
// .Head.Owner=owner and .Head.Generation=gen to be in this tree.
func (tree *RebuiltTree) isOwnerOK(owner btrfsprim.ObjID, gen btrfsprim.Generation) bool {
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
	for leaf, roots := range tree.acquireLeafToRoots(ctx) {
		if tree.Roots.HasAny(roots) == inc {
			leafs = append(leafs, leaf)
		}
	}
	tree.releaseLeafToRoots()
	slices.Sort(leafs)

	var stats rebuiltItemStats
	stats.Leafs.D = len(leafs)
	progressWriter := textui.NewProgress[rebuiltItemStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))

	var index containers.SortedMap[btrfsprim.Key, ItemPtr]
	for i, leaf := range leafs {
		stats.Leafs.N = i
		progressWriter.Set(stats)
		for j, itemKey := range tree.forrest.graph.Nodes[leaf].Items {
			newPtr := ItemPtr{
				Node: leaf,
				Slot: j,
			}
			if oldPtr, exists := index.Load(itemKey); !exists {
				index.Store(itemKey, newPtr)
				stats.NumItems++
			} else {
				if tree.RebuiltShouldReplace(oldPtr.Node, newPtr.Node) {
					index.Store(itemKey, newPtr)
				}
				stats.NumDups++
			}
			progressWriter.Set(stats)
		}
	}
	if stats.Leafs.N > 0 {
		stats.Leafs.N = stats.Leafs.D
		progressWriter.Set(stats)
		progressWriter.Done()
	}

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
	Leafs      textui.Portion[int]
	AddedLeafs int
	AddedItems int
}

func (s rebuiltRootStats) String() string {
	return textui.Sprintf("%v (added %v leafs, added %v items)",
		s.Leafs, s.AddedLeafs, s.AddedItems)
}

// RebuiltAddRoot adds an additional root node to the tree.  It is
// useful to call .RebuiltAddRoot() to re-attach part of the tree that
// has been broken off.
func (tree *RebuiltTree) RebuiltAddRoot(ctx context.Context, rootNode btrfsvol.LogicalAddr) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-tree.add-root", fmt.Sprintf("tree=%v rootNode=%v", tree.ID, rootNode))
	dlog.Info(ctx, "adding root...")

	var stats rebuiltRootStats
	leafToRoots := tree.acquireLeafToRoots(ctx)
	stats.Leafs.D = len(leafToRoots)
	progressWriter := textui.NewProgress[rebuiltRootStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	for i, leaf := range maps.SortedKeys(leafToRoots) {
		stats.Leafs.N = i
		progressWriter.Set(stats)

		if tree.Roots.HasAny(leafToRoots[leaf]) || !leafToRoots[leaf].Has(rootNode) {
			continue
		}

		stats.AddedLeafs++
		progressWriter.Set(stats)

		for _, itemKey := range tree.forrest.graph.Nodes[leaf].Items {
			tree.forrest.cb.AddedItem(ctx, tree.ID, itemKey)
			stats.AddedItems++
			progressWriter.Set(stats)
		}
	}
	stats.Leafs.N = len(leafToRoots)
	tree.releaseLeafToRoots()
	progressWriter.Set(stats)
	progressWriter.Done()

	tree.Roots.Insert(rootNode)
	tree.forrest.incItems.Delete(tree.ID) // force re-gen
	tree.forrest.excItems.Delete(tree.ID) // force re-gen

	if (tree.ID == btrfsprim.ROOT_TREE_OBJECTID || tree.ID == btrfsprim.UUID_TREE_OBJECTID) && stats.AddedItems > 0 {
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
	for root := range tree.acquireLeafToRoots(ctx)[leaf] {
		if tree.Roots.Has(root) {
			panic(fmt.Errorf("should not happen: (tree=%v).RebuiltLeafToRoots(leaf=%v): tree contains root=%v but not leaf",
				tree.ID, leaf, root))
		}
		ret.Insert(root)
	}
	tree.releaseLeafToRoots()
	if len(ret) == 0 {
		return nil
	}
	return ret
}
