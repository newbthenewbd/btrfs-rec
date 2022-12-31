// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrees

import (
	"context"
	"fmt"
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

	// all leafs (lvl=0) that pass .isOwnerOK, even if not in the tree
	leafToRoots map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]
	keys        containers.SortedMap[btrfsprim.Key, keyio.ItemPtr]

	// mutable
	Roots containers.Set[btrfsvol.LogicalAddr]
	Leafs containers.Set[btrfsvol.LogicalAddr]
	Items containers.SortedMap[btrfsprim.Key, keyio.ItemPtr]
}

// initializaton (called by `RebuiltForrest.Tree()`) ///////////////////////////////////////////////////////////////////

func (tree *RebuiltTree) indexLeafs(ctx context.Context) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.substep", "index-nodes")

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

	tree.leafToRoots = make(map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr])
	for node, roots := range nodeToRoots {
		if tree.forrest.graph.Nodes[node].Level == 0 && len(roots) > 0 {
			tree.leafToRoots[node] = roots
		}
	}
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

	// tree.keys
	for i, key := range tree.forrest.graph.Nodes[node].Items {
		if oldPtr, ok := tree.keys.Load(key); !ok || tree.shouldReplace(oldPtr.Node, node) {
			tree.keys.Store(key, keyio.ItemPtr{
				Node: node,
				Idx:  i,
			})
		}
	}
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

// .AddRoot() //////////////////////////////////////////////////////////////////////////////////////////////////////////

type rootStats struct {
	Leafs         textui.Portion[int]
	AddedItems    int
	ReplacedItems int
}

func (s rootStats) String() string {
	return textui.Sprintf("%v (added %v items, replaced %v items)",
		s.Leafs, s.AddedItems, s.ReplacedItems)
}

// AddRoot adds an additional root node to the tree.  It is useful to
// call .AddRoot() to re-attach part of the tree that has been broken
// off.
func (tree *RebuiltTree) AddRoot(ctx context.Context, rootNode btrfsvol.LogicalAddr) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-root", fmt.Sprintf("tree=%v rootNode=%v", tree.ID, rootNode))
	tree.Roots.Insert(rootNode)

	var stats rootStats
	stats.Leafs.D = len(tree.leafToRoots)
	progressWriter := textui.NewProgress[rootStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	for i, leaf := range maps.SortedKeys(tree.leafToRoots) {
		stats.Leafs.N = i
		progressWriter.Set(stats)
		if tree.Leafs.Has(leaf) || !tree.leafToRoots[leaf].Has(rootNode) {
			continue
		}
		tree.Leafs.Insert(leaf)
		for j, itemKey := range tree.forrest.graph.Nodes[leaf].Items {
			newPtr := keyio.ItemPtr{
				Node: leaf,
				Idx:  j,
			}
			if oldPtr, exists := tree.Items.Load(itemKey); !exists {
				tree.Items.Store(itemKey, newPtr)
				stats.AddedItems++
			} else if tree.shouldReplace(oldPtr.Node, newPtr.Node) {
				tree.Items.Store(itemKey, newPtr)
				stats.ReplacedItems++
			}
			tree.forrest.cbAddedItem(ctx, tree.ID, itemKey)
			progressWriter.Set(stats)
		}
	}
	stats.Leafs.N = len(tree.leafToRoots)
	progressWriter.Set(stats)
	progressWriter.Done()
}

func (tree *RebuiltTree) shouldReplace(oldNode, newNode btrfsvol.LogicalAddr) bool {
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
			// This is a panic because I'm not really sure what the best way to
			// handle this is, and so if this happens I want the program to crash
			// and force me to figure out how to handle it.
			panic(fmt.Errorf("dup nodes in tree=%v: old=%v=%v ; new=%v=%v",
				tree.ID,
				oldNode, tree.forrest.graph.Nodes[oldNode],
				newNode, tree.forrest.graph.Nodes[newNode]))
		}
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

// Resolve a key to a keyio.ItemPtr.
func (tree *RebuiltTree) Resolve(key btrfsprim.Key) (ptr keyio.ItemPtr, ok bool) {
	return tree.Items.Load(key)
}

// Load reads an item from a tree.
func (tree *RebuiltTree) Load(ctx context.Context, key btrfsprim.Key) (item btrfsitem.Item, ok bool) {
	ptr, ok := tree.Resolve(key)
	if !ok {
		return nil, false
	}
	return tree.forrest.keyIO.ReadItem(ctx, ptr)
}

// Search searches for an item from a tree.
func (tree *RebuiltTree) Search(fn func(btrfsprim.Key) int) (key btrfsprim.Key, ok bool) {
	k, _, ok := tree.Items.Search(func(k btrfsprim.Key, _ keyio.ItemPtr) int {
		return fn(k)
	})
	return k, ok
}

// Search searches for a range of items from a tree.
func (tree *RebuiltTree) SearchAll(fn func(btrfsprim.Key) int) []btrfsprim.Key {
	kvs := tree.Items.SearchAll(func(k btrfsprim.Key, _ keyio.ItemPtr) int {
		return fn(k)
	})
	if len(kvs) == 0 {
		return nil
	}
	ret := make([]btrfsprim.Key, len(kvs))
	for i := range kvs {
		ret[i] = kvs[i].K
	}
	return ret
}

// LeafToRoots returns the list of potential roots (to pass to
// .AddRoot) that include a given leaf-node.
func (tree *RebuiltTree) LeafToRoots(leaf btrfsvol.LogicalAddr) containers.Set[btrfsvol.LogicalAddr] {
	if tree.forrest.graph.Nodes[leaf].Level != 0 {
		panic(fmt.Errorf("should not happen: (tree=%v).LeafToRoots(leaf=%v): not a leaf",
			tree.ID, leaf))
	}
	ret := make(containers.Set[btrfsvol.LogicalAddr])
	for root := range tree.leafToRoots[leaf] {
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

// Keys returns a map of all keys in node that would be valid in this tree.
//
// Do not mutate the returned map; it is a pointer to the
// RebuiltTree's internal map!
func (tree *RebuiltTree) Keys() *containers.SortedMap[btrfsprim.Key, keyio.ItemPtr] {
	return &tree.keys
}
