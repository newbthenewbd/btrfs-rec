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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	pkggraph "git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/keyio"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type rebuiltTree struct {
	// static
	ID        btrfsprim.ObjID
	UUID      btrfsprim.UUID
	Parent    *rebuiltTree
	ParentGen btrfsprim.Generation // offset of this tree's root item

	// all leafs (lvl=0) that pass .isOwnerOK, even if not in the tree
	leafToRoots map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]
	keys        containers.SortedMap[btrfsprim.Key, keyio.ItemPtr]

	// mutable
	Roots containers.Set[btrfsvol.LogicalAddr]
	Leafs containers.Set[btrfsvol.LogicalAddr]
	Items containers.SortedMap[btrfsprim.Key, keyio.ItemPtr]
}

// isOwnerOK returns whether it is permissible for a node with
// .Head.Owner=owner to be in this tree.
func (tree *rebuiltTree) isOwnerOK(owner btrfsprim.ObjID, gen btrfsprim.Generation) bool {
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

// cowDistance returns how many COW-snapshots down the 'tree' is from
// the 'parent'.
func (tree *rebuiltTree) cowDistance(parentID btrfsprim.ObjID) (dist int, ok bool) {
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

func (tree *rebuiltTree) shouldReplace(graph pkggraph.Graph, oldNode, newNode btrfsvol.LogicalAddr) bool {
	oldDist, _ := tree.cowDistance(graph.Nodes[oldNode].Owner)
	newDist, _ := tree.cowDistance(graph.Nodes[newNode].Owner)
	switch {
	case newDist < oldDist:
		// Replace the old one with the new lower-dist one.
		return true
	case newDist > oldDist:
		// Retain the old lower-dist one.
		return false
	default:
		oldGen := graph.Nodes[oldNode].Generation
		newGen := graph.Nodes[newNode].Generation
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
				oldNode, graph.Nodes[oldNode],
				newNode, graph.Nodes[newNode]))
		}
	}
}

// RebuiltTrees is an abstraction for rebuilding and accessing
// potentially broken btrees.
//
// It is conceptually a btrfstree.TreeOperator, and adds similar
// broken-tree handling to btrfsutil.BrokenTrees.  However, the API is
// different thant btrfstree.TreeOperator, and is much more efficient
// than btrfsutil.BrokenTrees.
//
// The efficiency improvements are possible because of the API
// differences, which are necessary for how it is used in
// rebuildnodes:
//
//   - it consumes an already-read graph.Graph instead of reading the
//     graph itself
//
//   - it does not use `btrfstree.TreePath`
//
//   - it does not keep track of errors encountered in a tree
//
// Additionally, it provides some functionality that
// btrfsutil.BrokenTrees does not:
//
//   - it provides a .LeafToRoots() method to advise on what
//     additional roots should be added
//
//   - it provides a .COWDistance() method to compare how related two
//     trees are
//
// A zero RebuiltTrees is invalid; it must be initialized with
// NewRebuiltTrees().
type RebuiltTrees struct {
	// static
	sb    btrfstree.Superblock
	graph pkggraph.Graph
	keyIO *keyio.Handle

	// static callbacks
	cbAddedItem  func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key)
	cbLookupRoot func(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool)
	cbLookupUUID func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool)

	// mutable
	trees map[btrfsprim.ObjID]*rebuiltTree
}

// NewRebuiltTrees returns a new RebuiltTrees instance.  All of the
// callbacks must be non-nil.
func NewRebuiltTrees(
	sb btrfstree.Superblock, graph pkggraph.Graph, keyIO *keyio.Handle,
	cbAddedItem func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key),
	cbLookupRoot func(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool),
	cbLookupUUID func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool),
) *RebuiltTrees {
	return &RebuiltTrees{
		sb:    sb,
		graph: graph,
		keyIO: keyIO,

		cbAddedItem:  cbAddedItem,
		cbLookupRoot: cbLookupRoot,
		cbLookupUUID: cbLookupUUID,

		trees: make(map[btrfsprim.ObjID]*rebuiltTree),
	}
}

type rootStats struct {
	TreeID   btrfsprim.ObjID
	RootNode btrfsvol.LogicalAddr

	DoneLeafs     int
	TotalLeafs    int
	AddedItems    int
	ReplacedItems int
}

func (s rootStats) String() string {
	return fmt.Sprintf("tree %v: adding root node@%v: %v%% (%v/%v) (added %v items, replaced %v items)",
		s.TreeID, s.RootNode,
		int(100*float64(s.DoneLeafs)/float64(s.TotalLeafs)),
		s.DoneLeafs, s.TotalLeafs,
		s.AddedItems, s.ReplacedItems)
}

// AddRoot adds an additional root node to an existing tree.  It is
// useful to call .AddRoot() to re-attach part of the tree that has
// been broken off.
//
// It is invalid (panic) to call AddRoot for a tree without having
// called AddTree first.
func (ts *RebuiltTrees) AddRoot(ctx context.Context, treeID btrfsprim.ObjID, rootNode btrfsvol.LogicalAddr) {
	tree := ts.trees[treeID]
	tree.Roots.Insert(rootNode)

	progressWriter := textui.NewProgress[rootStats](ctx, dlog.LogLevelInfo, 1*time.Second)
	numAdded := 0
	numReplaced := 0
	progress := func(done int) {
		progressWriter.Set(rootStats{
			TreeID:        treeID,
			RootNode:      rootNode,
			DoneLeafs:     done,
			TotalLeafs:    len(tree.leafToRoots),
			AddedItems:    numAdded,
			ReplacedItems: numReplaced,
		})
	}
	for i, leaf := range maps.SortedKeys(tree.leafToRoots) {
		progress(i)
		if tree.Leafs.Has(leaf) || !tree.leafToRoots[leaf].Has(rootNode) {
			continue
		}
		tree.Leafs.Insert(leaf)
		for j, itemKey := range ts.graph.Nodes[leaf].Items {
			newPtr := keyio.ItemPtr{
				Node: leaf,
				Idx:  j,
			}
			if oldPtr, exists := tree.Items.Load(itemKey); !exists {
				tree.Items.Store(itemKey, newPtr)
				numAdded++
			} else if tree.shouldReplace(ts.graph, oldPtr.Node, newPtr.Node) {
				tree.Items.Store(itemKey, newPtr)
				numReplaced++
			}
			ts.cbAddedItem(ctx, treeID, itemKey)
			progress(i)
		}
	}
	progress(len(tree.leafToRoots))
	progressWriter.Done()
}

// AddTree initializes the given tree, returning true if it was able
// to do so, or false if there was a problem and nothing was done.
// The tree is initialized with the normal root node of the tree.
//
// Subsequent calls to AddTree for the same tree are no-ops.
func (ts *RebuiltTrees) AddTree(ctx context.Context, treeID btrfsprim.ObjID) (ok bool) {
	return ts.addTree(ctx, treeID, nil)
}

func (ts *RebuiltTrees) addTree(ctx context.Context, treeID btrfsprim.ObjID, stack []btrfsprim.ObjID) (ok bool) {
	if _, ok := ts.trees[treeID]; ok {
		return true
	}
	if slices.Contains(treeID, stack) {
		return false
	}

	tree := &rebuiltTree{
		ID:    treeID,
		Roots: make(containers.Set[btrfsvol.LogicalAddr]),
		Leafs: make(containers.Set[btrfsvol.LogicalAddr]),
	}
	var root btrfsvol.LogicalAddr
	switch treeID {
	case btrfsprim.ROOT_TREE_OBJECTID:
		root = ts.sb.RootTree
	case btrfsprim.CHUNK_TREE_OBJECTID:
		root = ts.sb.ChunkTree
	case btrfsprim.TREE_LOG_OBJECTID:
		root = ts.sb.LogTree
	case btrfsprim.BLOCK_GROUP_TREE_OBJECTID:
		root = ts.sb.BlockGroupRoot
	default:
		stack := append(stack, treeID)
		if !ts.addTree(ctx, btrfsprim.ROOT_TREE_OBJECTID, stack) {
			return false
		}
		rootOff, rootItem, ok := ts.cbLookupRoot(ctx, treeID)
		if !ok {
			return false
		}
		root = rootItem.ByteNr
		tree.UUID = rootItem.UUID
		if rootItem.ParentUUID != (btrfsprim.UUID{}) {
			tree.ParentGen = rootOff
			if !ts.addTree(ctx, btrfsprim.ROOT_TREE_OBJECTID, stack) {
				return false
			}
			parentID, ok := ts.cbLookupUUID(ctx, rootItem.ParentUUID)
			if !ok {
				return false
			}
			if !ts.addTree(ctx, parentID, append(stack, treeID)) {
				return false
			}
			tree.Parent = ts.trees[parentID]
		}
	}
	tree.indexLeafs(ctx, ts.graph)

	ts.trees[treeID] = tree
	if root != 0 {
		ts.AddRoot(ctx, treeID, root)
	}

	return true
}

type indexStats struct {
	TreeID     btrfsprim.ObjID
	DoneNodes  int
	TotalNodes int
}

func (s indexStats) String() string {
	return fmt.Sprintf("tree %v: indexing leaf nodes: %v%% (%v/%v)",
		s.TreeID,
		int(100*float64(s.DoneNodes)/float64(s.TotalNodes)),
		s.DoneNodes, s.TotalNodes)
}

func (tree *rebuiltTree) indexLeafs(ctx context.Context, graph pkggraph.Graph) {
	nodeToRoots := make(map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr])
	progressWriter := textui.NewProgress[indexStats](ctx, dlog.LogLevelInfo, 1*time.Second)
	progress := func() {
		progressWriter.Set(indexStats{
			TreeID:     tree.ID,
			DoneNodes:  len(nodeToRoots),
			TotalNodes: len(graph.Nodes),
		})
	}
	progress()
	for _, node := range maps.SortedKeys(graph.Nodes) {
		tree.indexNode(graph, node, nodeToRoots, progress, nil)
	}
	progressWriter.Done()

	tree.leafToRoots = make(map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr])
	for node, roots := range nodeToRoots {
		if graph.Nodes[node].Level == 0 && len(roots) > 0 {
			tree.leafToRoots[node] = roots
		}
	}
}

func (tree *rebuiltTree) indexNode(graph pkggraph.Graph, node btrfsvol.LogicalAddr, index map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr], progress func(), stack []btrfsvol.LogicalAddr) {
	defer progress()
	if _, done := index[node]; done {
		return
	}
	if slices.Contains(node, stack) {
		panic("loop")
	}
	if !tree.isOwnerOK(graph.Nodes[node].Owner, graph.Nodes[node].Generation) {
		index[node] = nil
		return
	}

	// tree.leafToRoots
	stack = append(stack, node)
	var roots containers.Set[btrfsvol.LogicalAddr]
	kps := slices.RemoveAllFunc(graph.EdgesTo[node], func(kp *pkggraph.Edge) bool {
		return !tree.isOwnerOK(graph.Nodes[kp.FromNode].Owner, graph.Nodes[kp.FromNode].Generation)
	})
	for _, kp := range kps {
		tree.indexNode(graph, kp.FromNode, index, progress, stack)
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
	for i, key := range graph.Nodes[node].Items {
		if oldPtr, ok := tree.keys.Load(key); !ok || tree.shouldReplace(graph, oldPtr.Node, node) {
			tree.keys.Store(key, keyio.ItemPtr{
				Node: node,
				Idx:  i,
			})
		}
	}
}

// Load reads an item from a tree.
//
// It is not nescessary to call AddTree for that tree first; Load will
// call it for you.
func (ts *RebuiltTrees) Load(ctx context.Context, treeID btrfsprim.ObjID, key btrfsprim.Key) (item btrfsitem.Item, ok bool) {
	if !ts.AddTree(ctx, treeID) {
		return nil, false
	}
	ptr, ok := ts.trees[treeID].Items.Load(key)
	if !ok {
		return nil, false
	}
	return ts.keyIO.ReadItem(ptr)
}

// Search searches for an item from a tree.
//
// It is not nescessary to call AddTree for that tree first; Search
// will call it for you.
func (ts *RebuiltTrees) Search(ctx context.Context, treeID btrfsprim.ObjID, fn func(btrfsprim.Key) int) (key btrfsprim.Key, ok bool) {
	if !ts.AddTree(ctx, treeID) {
		return btrfsprim.Key{}, false
	}
	k, _, ok := ts.trees[treeID].Items.Search(func(k btrfsprim.Key, _ keyio.ItemPtr) int {
		return fn(k)
	})
	return k, ok
}

// Search searches for a range of items from a tree.
//
// It is not nescessary to call AddTree for that tree first; SearchAll
// will call it for you.
func (ts *RebuiltTrees) SearchAll(ctx context.Context, treeID btrfsprim.ObjID, fn func(btrfsprim.Key) int) []btrfsprim.Key {
	if !ts.AddTree(ctx, treeID) {
		return nil
	}
	kvs := ts.trees[treeID].Items.SearchAll(func(k btrfsprim.Key, _ keyio.ItemPtr) int {
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
//
// It is not nescessary to call AddTree for the tree first;
// LeafToRoots will call it for you.
func (ts *RebuiltTrees) LeafToRoots(ctx context.Context, treeID btrfsprim.ObjID, leaf btrfsvol.LogicalAddr) containers.Set[btrfsvol.LogicalAddr] {
	if !ts.AddTree(ctx, treeID) {
		return nil
	}
	if ts.graph.Nodes[leaf].Level != 0 {
		panic(fmt.Errorf("should not happen: NodeToRoots(tree=%v, leaf=%v): not a leaf",
			treeID, leaf))
	}
	ret := make(containers.Set[btrfsvol.LogicalAddr])
	for root := range ts.trees[treeID].leafToRoots[leaf] {
		if ts.trees[treeID].Roots.Has(root) {
			panic(fmt.Errorf("should not happen: NodeToRoots(tree=%v, leaf=%v): tree contains root=%v but not leaf",
				treeID, leaf, root))
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
// It is invalid (panic) to call Keys for a tree without having called
// AddTree first.
func (ts *RebuiltTrees) Keys(treeID btrfsprim.ObjID) *containers.SortedMap[btrfsprim.Key, keyio.ItemPtr] {
	return &ts.trees[treeID].keys
}

// COWDistance returns how many COW-snapshots down from the 'child'
// tree is from the 'parent' tree.
//
// It is invalid (panic) to call COWDistance for a tree without having
// called AddTree for the child first.
func (ts *RebuiltTrees) COWDistance(ctx context.Context, childID, parentID btrfsprim.ObjID) (dist int, ok bool) {
	return ts.trees[childID].cowDistance(parentID)
}

// ListRoots returns a listing of all initialized trees and their root
// nodes.
//
// Do not mutate the set of roots for a tree; it is a pointer to the
// RebuiltTrees' internal set!
func (ts *RebuiltTrees) ListRoots() map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr] {
	ret := make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], len(ts.trees))
	for treeID := range ts.trees {
		ret[treeID] = ts.trees[treeID].Roots
	}
	return ret
}
