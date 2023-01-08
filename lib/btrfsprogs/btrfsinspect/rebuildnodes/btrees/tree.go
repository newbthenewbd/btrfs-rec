// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrees

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	pkggraph "git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/keyio"
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
	// `mu`; but they live in a shared LRUcache.  They are all
	// derived from tree.Roots, which is why it's OK if they get
	// evicted.
	//
	//  1. tree.leafToRoots()    = tree.forrest.leafs.Load(tree.ID)
	//  2. tree.Items()          = tree.forrest.incItems.Load(tree.ID)
	//  3. tree.PotentialItems() = tree.forrest.excItems.Load(tree.ID)
}

// LRU member 1: .leafToRoots() ////////////////////////////////////////////////////////////////////////////////////////

// leafToRoots returns all leafs (lvl=0) in the filesystem that pass
// .isOwnerOK, whether or not they're in the tree.
func (tree *RebuiltTree) leafToRoots(ctx context.Context) map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr] {
	return containers.LoadOrElse[btrfsprim.ObjID, map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]](&tree.forrest.leafs, tree.ID, func(btrfsprim.ObjID) map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr] {
		ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.index-nodes", fmt.Sprintf("tree=%v", tree.ID))

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
	})
}

func (tree *RebuiltTree) indexNode(ctx context.Context, node btrfsvol.LogicalAddr, index map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr], progress func(), stack []btrfsvol.LogicalAddr) {
	defer progress()
	if err := ctx.Err(); err != nil {
		return
	}
	if _, done := index[node]; done {
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
	kps := slices.RemoveAllFunc(tree.forrest.graph.EdgesTo[node], func(kp *pkggraph.Edge) bool {
		return !tree.isOwnerOK(tree.forrest.graph.Nodes[kp.FromNode].Owner, tree.forrest.graph.Nodes[kp.FromNode].Generation)
	})
	for _, kp := range kps {
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
// .Head.Owner=owner to be in this tree.
func (tree *RebuiltTree) isOwnerOK(owner btrfsprim.ObjID, gen btrfsprim.Generation) bool {
	for {
		if owner == tree.ID {
			return true
		}
		if tree.Parent == nil || gen >= tree.ParentGen {
			return false
		}
		tree = tree.Parent
	}
}

// LRU members 2 and 3: .Items() and .PotentialItems() /////////////////////////////////////////////////////////////////

// Items returns a map of the items contained in this tree.
//
// Do not mutate the returned map; it is a pointer to the
// RebuiltTree's internal map!
func (tree *RebuiltTree) Items(ctx context.Context) *containers.SortedMap[btrfsprim.Key, keyio.ItemPtr] {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.index-inc-items", fmt.Sprintf("tree=%v", tree.ID))
	return tree.items(ctx, &tree.forrest.incItems, tree.Roots.HasAny)
}

// PotentialItems returns a map of items that could be added to this
// tree with .AddRoot().
//
// Do not mutate the returned map; it is a pointer to the
// RebuiltTree's internal map!
func (tree *RebuiltTree) PotentialItems(ctx context.Context) *containers.SortedMap[btrfsprim.Key, keyio.ItemPtr] {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.index-exc-items", fmt.Sprintf("tree=%v", tree.ID))
	return tree.items(ctx, &tree.forrest.excItems,
		func(roots containers.Set[btrfsvol.LogicalAddr]) bool {
			return !tree.Roots.HasAny(roots)
		})
}

type itemIndex = containers.SortedMap[btrfsprim.Key, keyio.ItemPtr]

type itemStats struct {
	Leafs    textui.Portion[int]
	NumItems int
	NumDups  int
}

func (s itemStats) String() string {
	return textui.Sprintf("%v (%v items, %v dups)",
		s.Leafs, s.NumItems, s.NumDups)
}

func (tree *RebuiltTree) items(ctx context.Context, cache containers.Map[btrfsprim.ObjID, *itemIndex],
	leafFn func(roots containers.Set[btrfsvol.LogicalAddr]) bool,
) *containers.SortedMap[btrfsprim.Key, keyio.ItemPtr] {
	tree.mu.RLock()
	defer tree.mu.RUnlock()

	return containers.LoadOrElse(cache, tree.ID, func(btrfsprim.ObjID) *itemIndex {
		var leafs []btrfsvol.LogicalAddr
		for leaf, roots := range tree.leafToRoots(ctx) {
			if leafFn(roots) {
				leafs = append(leafs, leaf)
			}
		}
		slices.Sort(leafs)

		var stats itemStats
		stats.Leafs.D = len(leafs)
		progressWriter := textui.NewProgress[itemStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))

		index := new(containers.SortedMap[btrfsprim.Key, keyio.ItemPtr])
		for i, leaf := range leafs {
			stats.Leafs.N = i
			progressWriter.Set(stats)
			for j, itemKey := range tree.forrest.graph.Nodes[leaf].Items {
				newPtr := keyio.ItemPtr{
					Node: leaf,
					Idx:  j,
				}
				if oldPtr, exists := index.Load(itemKey); !exists {
					index.Store(itemKey, newPtr)
					stats.NumItems++
				} else {
					if tree.ShouldReplace(oldPtr.Node, newPtr.Node) {
						index.Store(itemKey, newPtr)
					}
					stats.NumDups++
				}
				progressWriter.Set(stats)
			}
		}
		if stats.Leafs.N > 0 {
			stats.Leafs.N = len(leafs)
			progressWriter.Set(stats)
			progressWriter.Done()
		}

		return index
	})
}

func (tree *RebuiltTree) ShouldReplace(oldNode, newNode btrfsvol.LogicalAddr) bool {
	oldDist, _ := tree.COWDistance(tree.forrest.graph.Nodes[oldNode].Owner)
	newDist, _ := tree.COWDistance(tree.forrest.graph.Nodes[newNode].Owner)
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

// .AddRoot() //////////////////////////////////////////////////////////////////////////////////////////////////////////

type rootStats struct {
	Leafs      textui.Portion[int]
	AddedLeafs int
	AddedItems int
}

func (s rootStats) String() string {
	return textui.Sprintf("%v (added %v leafs, added %v items)",
		s.Leafs, s.AddedLeafs, s.AddedItems)
}

// AddRoot adds an additional root node to the tree.  It is useful to
// call .AddRoot() to re-attach part of the tree that has been broken
// off.
func (tree *RebuiltTree) AddRoot(ctx context.Context, rootNode btrfsvol.LogicalAddr) {
	tree.mu.Lock()
	defer tree.mu.Unlock()
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-root", fmt.Sprintf("tree=%v rootNode=%v", tree.ID, rootNode))
	dlog.Info(ctx, "adding root...")

	leafToRoots := tree.leafToRoots(ctx)

	var stats rootStats
	stats.Leafs.D = len(leafToRoots)
	progressWriter := textui.NewProgress[rootStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
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
	progressWriter.Set(stats)
	progressWriter.Done()

	tree.Roots.Insert(rootNode)
	tree.forrest.incItems.Delete(tree.ID) // force re-gen
	tree.forrest.excItems.Delete(tree.ID) // force re-gen

	if (tree.ID == btrfsprim.ROOT_TREE_OBJECTID || tree.ID == btrfsprim.UUID_TREE_OBJECTID) && stats.AddedItems > 0 {
		tree.forrest.trees.Range(func(otherTreeID btrfsprim.ObjID, otherTree *RebuiltTree) bool {
			if otherTree == nil {
				tree.forrest.trees.Delete(otherTreeID)
			}
			return true
		})
	}
}

// main public API /////////////////////////////////////////////////////////////////////////////////////////////////////

// COWDistance returns how many COW-snapshots down the 'tree' is from
// the 'parent'.
func (tree *RebuiltTree) COWDistance(parentID btrfsprim.ObjID) (dist int, ok bool) {
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
	ptr, ok := tree.Items(ctx).Load(key)
	if !ok {
		panic(fmt.Errorf("should not happen: btrees.RebuiltTree.ReadItem called for not-included key: %v", key))
	}
	return tree.forrest.keyIO.ReadItem(ctx, ptr)
}

// LeafToRoots returns the list of potential roots (to pass to
// .AddRoot) that include a given leaf-node.
func (tree *RebuiltTree) LeafToRoots(ctx context.Context, leaf btrfsvol.LogicalAddr) containers.Set[btrfsvol.LogicalAddr] {
	if tree.forrest.graph.Nodes[leaf].Level != 0 {
		panic(fmt.Errorf("should not happen: (tree=%v).LeafToRoots(leaf=%v): not a leaf",
			tree.ID, leaf))
	}
	tree.mu.RLock()
	defer tree.mu.RUnlock()
	ret := make(containers.Set[btrfsvol.LogicalAddr])
	for root := range tree.leafToRoots(ctx)[leaf] {
		if tree.Roots.Has(root) {
			panic(fmt.Errorf("should not happen: (tree=%v).LeafToRoots(leaf=%v): tree contains root=%v but not leaf",
				tree.ID, leaf, root))
		}
		ret.Insert(root)
	}
	if len(ret) == 0 {
		return nil
	}
	return ret
}
