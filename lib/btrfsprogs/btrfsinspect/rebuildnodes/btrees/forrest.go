// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrees

import (
	"context"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	pkggraph "git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/keyio"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

// RebuiltForrest is an abstraction for rebuilding and accessing
// potentially broken btrees.
//
// It is conceptually a btrfstree.TreeOperator, and adds similar
// broken-tree handling to btrfsutil.BrokenForrest.  However, the API
// is different thant btrfstree.TreeOperator, and is much more
// efficient than btrfsutil.BrokenForrest.
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
// btrfsutil.BrokenForrest does not:
//
//   - it provides a .LeafToRoots() method to advise on what
//     additional roots should be added
//
//   - it provides a .COWDistance() method to compare how related two
//     trees are
//
// A zero RebuiltForrest is invalid; it must be initialized with
// NewRebuiltForrest().
type RebuiltForrest struct {
	// static
	sb    btrfstree.Superblock
	graph pkggraph.Graph
	keyIO *keyio.Handle

	// static callbacks
	cbAddedItem  func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key)
	cbLookupRoot func(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool)
	cbLookupUUID func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool)

	// mutable
	trees    map[btrfsprim.ObjID]*RebuiltTree
	allItems *containers.LRUCache[btrfsprim.ObjID, *itemIndex]
	incItems *containers.LRUCache[btrfsprim.ObjID, *itemIndex]
}

// NewRebuiltForrest returns a new RebuiltForrest instance.  All of
// the callbacks must be non-nil.
func NewRebuiltForrest(
	sb btrfstree.Superblock, graph pkggraph.Graph, keyIO *keyio.Handle,
	cbAddedItem func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key),
	cbLookupRoot func(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool),
	cbLookupUUID func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool),
) *RebuiltForrest {
	return &RebuiltForrest{
		sb:    sb,
		graph: graph,
		keyIO: keyIO,

		cbAddedItem:  cbAddedItem,
		cbLookupRoot: cbLookupRoot,
		cbLookupUUID: cbLookupUUID,

		trees:    make(map[btrfsprim.ObjID]*RebuiltTree),
		allItems: containers.NewLRUCache[btrfsprim.ObjID, *itemIndex](textui.Tunable(8)),
		incItems: containers.NewLRUCache[btrfsprim.ObjID, *itemIndex](textui.Tunable(8)),
	}
}

// Tree returns a given tree, initializing it if nescessary.  If it is
// unable to initialize the tree, then nil is returned, and nothing is
// done to the forrest.
//
// The tree is initialized with the normal root node of the tree.
func (ts *RebuiltForrest) Tree(ctx context.Context, treeID btrfsprim.ObjID) *RebuiltTree {
	if !ts.addTree(ctx, treeID, nil) {
		return nil
	}
	return ts.trees[treeID]
}

func (ts *RebuiltForrest) addTree(ctx context.Context, treeID btrfsprim.ObjID, stack []btrfsprim.ObjID) (ok bool) {
	if _, ok := ts.trees[treeID]; ok {
		return true
	}
	if slices.Contains(treeID, stack) {
		return false
	}
	stack = append(stack, treeID)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree", stack)
	dlog.Info(ctx, "adding tree...")

	tree := &RebuiltTree{
		ID:      treeID,
		Roots:   make(containers.Set[btrfsvol.LogicalAddr]),
		Leafs:   make(containers.Set[btrfsvol.LogicalAddr]),
		forrest: ts,
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
			if !ts.addTree(ctx, btrfsprim.UUID_TREE_OBJECTID, stack) {
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
	tree.indexLeafs(ctx)

	ts.trees[treeID] = tree
	if root != 0 {
		tree.AddRoot(ctx, root)
	}

	return true
}

// ListRoots returns a listing of all initialized trees and their root
// nodes.
//
// Do not mutate the set of roots for a tree; it is a pointer to the
// RebuiltForrest's internal set!
func (ts *RebuiltForrest) ListRoots() map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr] {
	ret := make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], len(ts.trees))
	for treeID := range ts.trees {
		ret[treeID] = ts.trees[treeID].Roots
	}
	return ret
}
