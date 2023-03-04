// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

// RebuiltForrest is an abstraction for rebuilding and accessing
// potentially broken btrees.
//
// It is conceptually a btrfstree.Forrest, and adds similar
// broken-tree handling to OldRebuiltForrest.  However, it is much
// more efficient than OldRebuiltForrest.
//
// The efficiency improvements are possible because of the API
// differences, which are necessary for how it is used in
// rebuildtrees:
//
//   - it consumes an already-read Graph instead of reading the graph
//     itself
//
//   - it does not use `btrfstree.Path`
//
//   - it does not keep track of errors encountered in a tree
//
// Additionally, it provides some functionality that OldRebuiltForrest
// does not:
//
//   - it provides a RebuiltForrest.RebuiltListRoots() method for
//     listing how trees have been repaired.
//
//   - it provides a RebuiltTree.RebuiltAddRoot() method for repairing a
//     tree.
//
//   - it provides several RebuiltTree methods that provide advice on
//     what roots should be added to a tree in order to repair it:
//
//     .RebuiltAcquireItems()/.RebuiltReleaseItems() and
//     .RebuiltAcquirePotentialItems()/.RebuiltReleasePotentialItems()
//     to compare what's in the tree and what could be in the tree.
//
//     .RebuiltLeafToRoots() to map potential items to things that can
//     be passed to .RebuiltAddRoot().
//
//     .RebuiltCOWDistance() and .RebuiltShouldReplace() to provide
//     information on deciding on an option from
//     .RebuiltLeafToRoots().
//
// A zero RebuiltForrest is invalid; it must be initialized with
// NewRebuiltForrest().
type RebuiltForrest struct {
	// static
	inner btrfs.ReadableFS
	graph Graph
	cb    RebuiltForrestCallbacks

	// mutable

	treesMu nestedMutex
	trees   map[btrfsprim.ObjID]*RebuiltTree // must hold .treesMu to access

	rebuiltSharedCache
}

// NewRebuiltForrest returns a new RebuiltForrest instance.
//
// The `cb` RebuiltForrestCallbacks may be nil.  If `cb` also
// implements RebuiltForrestExtendedCallbacks, then a series of
// .AddedItem() calls will be made before each call to .AddedRoot().
func NewRebuiltForrest(fs btrfs.ReadableFS, graph Graph, cb RebuiltForrestCallbacks) *RebuiltForrest {
	ret := &RebuiltForrest{
		inner: fs,
		graph: graph,
		cb:    cb,

		trees: make(map[btrfsprim.ObjID]*RebuiltTree),
	}

	ret.rebuiltSharedCache = makeRebuiltSharedCache(ret)

	if ret.cb == nil {
		ret.cb = noopRebuiltForrestCallbacks{
			forrest: ret,
		}
	}
	return ret
}

// RebuiltTree returns a given tree, initializing it if nescessary.
//
// The tree is initialized with the normal root node of the tree.
//
// This is identical to .ForrestLookup(), but returns a concrete type
// rather than an interface.
func (ts *RebuiltForrest) RebuiltTree(ctx context.Context, treeID btrfsprim.ObjID) (*RebuiltTree, error) {
	ctx = ts.treesMu.Lock(ctx)
	defer ts.treesMu.Unlock()
	ts.rebuildTree(ctx, treeID, nil)
	tree := ts.trees[treeID]
	if tree.ancestorLoop && tree.rootErr == nil {
		var loop []btrfsprim.ObjID
		for ancestor := tree; true; ancestor = ancestor.Parent {
			loop = append(loop, ancestor.ID)
			if slices.Contains(ancestor.ID, loop[:len(loop)-1]) {
				break
			}
		}
		tree.rootErr = fmt.Errorf("loop detected: %v", loop)
	}
	if tree.rootErr != nil {
		return nil, tree.rootErr
	}
	return tree, nil
}

func (ts *RebuiltForrest) rebuildTree(ctx context.Context, treeID btrfsprim.ObjID, stack []btrfsprim.ObjID) {
	loop := false
	if maps.HasKey(ts.trees, treeID) {
		loop = slices.Contains(treeID, stack)
		if !loop {
			return
		}
	}

	stack = append(stack, treeID)
	ctx = dlog.WithField(ctx, "btrfs.util.rebuilt-forrest.add-tree", stack)
	defer func() {
		if ts.trees[treeID].rootErr != nil {
			dlog.Errorf(ctx, "failed to add tree: %v", ts.trees[treeID].rootErr)
		}
	}()
	dlog.Info(ctx, "adding tree...")

	if loop {
		ts.trees[treeID].ancestorLoop = true
		dlog.Error(ctx, "loop detected")
		return
	}

	ts.trees[treeID] = &RebuiltTree{
		ID:      treeID,
		Roots:   make(containers.Set[btrfsvol.LogicalAddr]),
		forrest: ts,
	}
	var root btrfsvol.LogicalAddr
	switch treeID {
	case btrfsprim.ROOT_TREE_OBJECTID:
		sb, _ := ts.inner.Superblock()
		root = sb.RootTree
	case btrfsprim.CHUNK_TREE_OBJECTID:
		sb, _ := ts.inner.Superblock()
		root = sb.ChunkTree
	case btrfsprim.TREE_LOG_OBJECTID:
		sb, _ := ts.inner.Superblock()
		root = sb.LogTree
	case btrfsprim.BLOCK_GROUP_TREE_OBJECTID:
		sb, _ := ts.inner.Superblock()
		root = sb.BlockGroupRoot
	default:
		rootOff, rootItem, ok := ts.cb.LookupRoot(ctx, treeID)
		if !ok {
			ts.trees[treeID].rootErr = btrfstree.ErrNoTree
			return
		}
		root = rootItem.ByteNr
		ts.trees[treeID].UUID = rootItem.UUID
		if rootItem.ParentUUID != (btrfsprim.UUID{}) {
			ts.trees[treeID].ParentGen = rootOff
			parentID, ok := ts.cb.LookupUUID(ctx, rootItem.ParentUUID)
			if !ok {
				ts.trees[treeID].rootErr = fmt.Errorf("failed to look up UUID: %v", rootItem.ParentUUID)
				return
			}
			ts.rebuildTree(ctx, parentID, stack)
			ts.trees[treeID].Parent = ts.trees[parentID]
			switch {
			case ts.trees[treeID].Parent.ancestorLoop:
				ts.trees[treeID].ancestorLoop = true
				return
			case ts.trees[treeID].Parent.rootErr != nil:
				ts.trees[treeID].rootErr = fmt.Errorf("failed to rebuild parent tree: %v: %w", parentID, ts.trees[treeID].Parent.rootErr)
				return
			}
		}
	}

	if root != 0 {
		ts.trees[treeID].RebuiltAddRoot(ctx, root)
	}
}

func (ts *RebuiltForrest) flushNegativeCache(ctx context.Context) {
	_ = ts.treesMu.Lock(ctx)
	defer ts.treesMu.Unlock()
	for treeID, tree := range ts.trees {
		if tree.rootErr != nil || tree.ancestorLoop {
			delete(ts.trees, treeID)
		}
	}
}

// RebuiltListRoots returns a listing of all initialized trees and
// their root nodes.
//
// Do not mutate the set of roots for a tree; it is a pointer to the
// RebuiltForrest's internal set!
func (ts *RebuiltForrest) RebuiltListRoots(ctx context.Context) map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr] {
	_ = ts.treesMu.Lock(ctx)
	defer ts.treesMu.Unlock()
	ret := make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr])
	for treeID, tree := range ts.trees {
		if len(tree.Roots) > 0 {
			ret[treeID] = tree.Roots
		}
	}
	return ret
}

// btrfs.ReadableFS interface //////////////////////////////////////////////////////////////////////////////////////////

var _ btrfs.ReadableFS = (*RebuiltForrest)(nil)

// Name implements btrfs.ReadableFS.
func (ts *RebuiltForrest) Name() string {
	return ts.inner.Name()
}

// ForrestLookup implements btrfstree.Forrest (and btrfs.ReadableFS).
//
// It is identical to .RebuiltTree(), but returns an interface rather
// than a concrete type.
func (ts *RebuiltForrest) ForrestLookup(ctx context.Context, treeID btrfsprim.ObjID) (btrfstree.Tree, error) {
	return ts.RebuiltTree(ctx, treeID)
}

// Superblock implements btrfstree.NodeSource (and btrfs.ReadableFS).
func (ts *RebuiltForrest) Superblock() (*btrfstree.Superblock, error) {
	return ts.inner.Superblock()
}

// AcquireNode implements btrfstree.NodeSource (and btrfs.ReadableFS).
func (ts *RebuiltForrest) AcquireNode(ctx context.Context, addr btrfsvol.LogicalAddr, exp btrfstree.NodeExpectations) (*btrfstree.Node, error) {
	return ts.inner.AcquireNode(ctx, addr, exp)
}

// ReleaseNode implements btrfstree.NodeSource (and btrfs.ReadableFS).
func (ts *RebuiltForrest) ReleaseNode(node *btrfstree.Node) {
	ts.inner.ReleaseNode(node)
}

// ReadAt implements diskio.ReaderAt[btrfsvol.LogicalAddr] (and btrfs.ReadableFS).
func (ts *RebuiltForrest) ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return ts.inner.ReadAt(p, off)
}
