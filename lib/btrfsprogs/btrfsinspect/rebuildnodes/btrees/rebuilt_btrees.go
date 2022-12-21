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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type itemPtr struct {
	Node btrfsvol.LogicalAddr
	Idx  int
}

type rebuiltTree struct {
	// static
	ID     btrfsprim.ObjID
	UUID   btrfsprim.UUID
	Parent *rebuiltTree

	// mutable
	Roots containers.Set[btrfsvol.LogicalAddr]
	Items containers.SortedMap[btrfsprim.Key, itemPtr]
}

// isOwnerOK returns whether it is permissible for a node with
// .Head.Owner=owner to be in this tree.
func (t *rebuiltTree) isOwnerOK(owner btrfsprim.ObjID) bool {
	for {
		if t == nil {
			return false
		}
		if owner == t.ID {
			return true
		}
		t = t.Parent
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
// A zero RebuiltTrees is invalid; it must be initialized with
// NewRebuiltTrees().
type RebuiltTrees struct {
	// static
	rawFile diskio.File[btrfsvol.LogicalAddr]
	sb      btrfstree.Superblock
	graph   graph.Graph

	// static callbacks
	cbAddedItem  func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key)
	cbLookupRoot func(ctx context.Context, tree btrfsprim.ObjID) (item btrfsitem.Root, ok bool)
	cbLookupUUID func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool)

	// mutable
	trees     map[btrfsprim.ObjID]*rebuiltTree
	nodeCache *containers.LRUCache[btrfsvol.LogicalAddr, *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]]
}

// NewRebuiltTrees returns a new RebuiltTrees instance.  All of the
// callbacks must be non-nil.
func NewRebuiltTrees(
	file diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock, graph graph.Graph,
	cbAddedItem func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key),
	cbLookupRoot func(ctx context.Context, tree btrfsprim.ObjID) (item btrfsitem.Root, ok bool),
	cbLookupUUID func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool),
) *RebuiltTrees {
	return &RebuiltTrees{
		rawFile: file,
		sb:      sb,
		graph:   graph,

		cbAddedItem:  cbAddedItem,
		cbLookupRoot: cbLookupRoot,
		cbLookupUUID: cbLookupUUID,

		trees:     make(map[btrfsprim.ObjID]*rebuiltTree),
		nodeCache: containers.NewLRUCache[btrfsvol.LogicalAddr, *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]](8),
	}
}

func (ts *RebuiltTrees) readNode(laddr btrfsvol.LogicalAddr) *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node] {
	if cached, ok := ts.nodeCache.Get(laddr); ok {
		return cached
	}

	graphInfo, ok := ts.graph.Nodes[laddr]
	if !ok {
		panic(fmt.Errorf("should not happen: node@%v is not mentioned in the in-memory graph", laddr))
	}

	ref, err := btrfstree.ReadNode(ts.rawFile, ts.sb, laddr, btrfstree.NodeExpectations{
		LAddr:      containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
		Level:      containers.Optional[uint8]{OK: true, Val: graphInfo.Level},
		Generation: containers.Optional[btrfsprim.Generation]{OK: true, Val: graphInfo.Generation},
		Owner: func(treeID btrfsprim.ObjID) error {
			if treeID != graphInfo.Owner {
				return fmt.Errorf("expected owner=%v but claims to have owner=%v",
					graphInfo.Owner, treeID)
			}
			return nil
		},
		MinItem: containers.Optional[btrfsprim.Key]{OK: true, Val: graphInfo.MinItem},
		MaxItem: containers.Optional[btrfsprim.Key]{OK: true, Val: graphInfo.MaxItem},
	})
	if err != nil {
		panic(fmt.Errorf("should not happen: i/o error: %w", err))
	}

	ts.nodeCache.Add(laddr, ref)

	return ref
}

func walk(graph graph.Graph, root btrfsvol.LogicalAddr, visited containers.Set[btrfsvol.LogicalAddr], fn func(btrfsvol.LogicalAddr) bool) {
	if _, ok := graph.Nodes[root]; !ok {
		return
	}
	if visited.Has(root) {
		return
	}
	defer visited.Insert(root)
	if !fn(root) {
		return
	}
	for _, kp := range graph.EdgesFrom[root] {
		walk(graph, kp.ToNode, visited, fn)
	}
}

type rootStats struct {
	TreeID   btrfsprim.ObjID
	RootNode btrfsvol.LogicalAddr

	VisitedNodes int
	TotalNodes   int
	AddedItems   int
}

func (s rootStats) String() string {
	return fmt.Sprintf("tree %v: adding root node@%v: %v%% (%v/%v) (added %v items)",
		s.TreeID, s.RootNode,
		int(100*float64(s.VisitedNodes)/float64(s.TotalNodes)),
		s.VisitedNodes, s.TotalNodes,
		s.AddedItems)
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

	visited := make(containers.Set[btrfsvol.LogicalAddr])
	progressWriter := textui.NewProgress[rootStats](ctx, dlog.LogLevelInfo, 1*time.Second)
	numAdded := 0
	progress := func() {
		progressWriter.Set(rootStats{
			TreeID:       treeID,
			RootNode:     rootNode,
			VisitedNodes: len(visited),
			TotalNodes:   len(ts.graph.Nodes),
			AddedItems:   numAdded,
		})
	}
	progress()
	walk(ts.graph, rootNode, visited, func(node btrfsvol.LogicalAddr) bool {
		progress()
		if !tree.isOwnerOK(ts.graph.Nodes[node].Owner) {
			return false
		}
		if ts.graph.Nodes[node].Level == 0 {
			for i, item := range ts.readNode(node).Data.BodyLeaf {
				if _, exists := tree.Items.Load(item.Key); exists {
					// This is a panic because I'm not really sure what the best way to
					// handle this is, and so if this happens I want the program to crash
					// and force me to figure out how to handle it.
					panic(fmt.Errorf("dup key=%v in tree=%v", item.Key, treeID))
				}
				tree.Items.Store(item.Key, itemPtr{
					Node: node,
					Idx:  i,
				})
				numAdded++
				ts.cbAddedItem(ctx, treeID, item.Key)
			}
		}
		return true
	})
	progress()
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
		rootItem, ok := ts.cbLookupRoot(ctx, treeID)
		if !ok {
			return false
		}
		root = rootItem.ByteNr
		tree.UUID = rootItem.UUID
		if rootItem.ParentUUID != (btrfsprim.UUID{}) {
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
	ts.trees[treeID] = tree
	if root != 0 {
		ts.AddRoot(ctx, treeID, root)
	}
	return true
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
	return ts.readNode(ptr.Node).Data.BodyLeaf[ptr.Idx].Body, true
}

// Search searches for an item from a tree.
//
// It is not nescessary to call AddTree for that tree first; Search
// will call it for you.
func (ts *RebuiltTrees) Search(ctx context.Context, treeID btrfsprim.ObjID, fn func(btrfsprim.Key) int) (key btrfsprim.Key, ok bool) {
	if !ts.AddTree(ctx, treeID) {
		return btrfsprim.Key{}, false
	}
	k, _, ok := ts.trees[treeID].Items.Search(func(k btrfsprim.Key, _ itemPtr) int {
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
	kvs := ts.trees[treeID].Items.SearchAll(func(k btrfsprim.Key, _ itemPtr) int {
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
