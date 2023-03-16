// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	"sync"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type oldRebuiltTree struct {
	RootErr error
	Items   *containers.RBTree[oldRebuiltTreeValue]
	Errors  *containers.IntervalTree[btrfsprim.Key, oldRebuiltTreeError]
}

type oldRebuiltTreeError struct {
	Path SkinnyPath
	Err  error
}

type oldRebuiltTreeValue struct {
	Path     SkinnyPath
	Key      btrfsprim.Key
	ItemSize uint32
}

// Compare implements containers.Ordered.
func (a oldRebuiltTreeValue) Compare(b oldRebuiltTreeValue) int {
	return a.Key.Compare(b.Key)
}

func newOldRebuiltTree(arena *SkinnyPathArena) oldRebuiltTree {
	return oldRebuiltTree{
		Items: new(containers.RBTree[oldRebuiltTreeValue]),
		Errors: &containers.IntervalTree[btrfsprim.Key, oldRebuiltTreeError]{
			MinFn: func(err oldRebuiltTreeError) btrfsprim.Key {
				return arena.Inflate(err.Path).Node(-1).ToKey
			},
			MaxFn: func(err oldRebuiltTreeError) btrfsprim.Key {
				return arena.Inflate(err.Path).Node(-1).ToMaxKey
			},
		},
	}
}

type OldRebuiltForrest struct {
	ctx   context.Context //nolint:containedctx // don't have an option while keeping the same API
	inner *btrfs.FS

	arena *SkinnyPathArena

	// btrfsprim.ROOT_TREE_OBJECTID
	rootTreeMu sync.Mutex
	rootTree   *oldRebuiltTree
	// for all other trees
	treesMu sync.Mutex
	trees   map[btrfsprim.ObjID]oldRebuiltTree
}

var _ btrfstree.TreeOperator = (*OldRebuiltForrest)(nil)

// NewOldRebuiltForrest wraps a *btrfs.FS to support looking up
// information from broken trees.
//
// Of the btrfstree.TreeOperator methods:
//
//   - TreeWalk works on broken trees
//   - TreeLookup relies on the tree being properly ordered (which a
//     broken tree might not be).
//   - TreeSearch relies on the tree being properly ordered (which a
//     broken tree might not be).
//   - TreeSearchAll relies on the tree being properly ordered (which a
//     broken tree might not be), and a bad node may cause it to not
//     return a truncated list of results.
//
// NewOldRebuiltForrest attempts to remedy these deficiencies by using
// .TreeWalk to build an out-of-FS index of all of the items in the
// tree, and re-implements TreeLookup, TreeSearch, and TreeSearchAll
// using that index.
func NewOldRebuiltForrest(ctx context.Context, inner *btrfs.FS) *OldRebuiltForrest {
	return &OldRebuiltForrest{
		ctx:   ctx,
		inner: inner,
	}
}

func (bt *OldRebuiltForrest) RebuiltTree(treeID btrfsprim.ObjID) oldRebuiltTree {
	var treeRoot *btrfstree.TreeRoot
	var sb *btrfstree.Superblock
	var err error
	if treeID == btrfsprim.ROOT_TREE_OBJECTID {
		bt.rootTreeMu.Lock()
		defer bt.rootTreeMu.Unlock()
		if bt.rootTree != nil {
			return *bt.rootTree
		}
		sb, err = bt.inner.Superblock()
		if err == nil {
			treeRoot, err = btrfstree.LookupTreeRoot(bt.inner, *sb, treeID)
		}
	} else {
		bt.treesMu.Lock()
		defer bt.treesMu.Unlock()
		if bt.trees == nil {
			bt.trees = make(map[btrfsprim.ObjID]oldRebuiltTree)
		}
		if cacheEntry, exists := bt.trees[treeID]; exists {
			return cacheEntry
		}
		sb, err = bt.inner.Superblock()
		if err == nil {
			treeRoot, err = btrfstree.LookupTreeRoot(bt, *sb, treeID)
		}
	}
	if bt.arena == nil {
		var _sb btrfstree.Superblock
		if sb != nil {
			_sb = *sb
		}
		bt.arena = &SkinnyPathArena{
			FS: bt.inner,
			SB: _sb,
		}
	}
	cacheEntry := newOldRebuiltTree(bt.arena)
	if err != nil {
		cacheEntry.RootErr = err
	} else {
		dlog.Infof(bt.ctx, "indexing tree %v...", treeID)
		bt.rawTreeWalk(*treeRoot, cacheEntry, nil)
		dlog.Infof(bt.ctx, "... done indexing tree %v", treeID)
	}
	if treeID == btrfsprim.ROOT_TREE_OBJECTID {
		bt.rootTree = &cacheEntry
	} else {
		bt.trees[treeID] = cacheEntry
	}
	return cacheEntry
}

func (bt *OldRebuiltForrest) rawTreeWalk(root btrfstree.TreeRoot, cacheEntry oldRebuiltTree, walked *[]btrfsprim.Key) {
	errHandle := func(err *btrfstree.TreeError) {
		if len(err.Path) > 0 && err.Path.Node(-1).ToNodeAddr == 0 {
			// This is a panic because on the filesystems I'm working with it more likely
			// indicates a bug in my item parser than a problem with the filesystem.
			panic(fmt.Errorf("TODO: error parsing item: %w", err))
		}
		cacheEntry.Errors.Insert(oldRebuiltTreeError{
			Path: bt.arena.Deflate(err.Path),
			Err:  err.Err,
		})
	}

	cbs := btrfstree.TreeWalkHandler{
		Item: func(path btrfstree.TreePath, item btrfstree.Item) error {
			if cacheEntry.Items.Search(func(v oldRebuiltTreeValue) int { return item.Key.Compare(v.Key) }) != nil {
				// This is a panic because I'm not really sure what the best way to
				// handle this is, and so if this happens I want the program to crash
				// and force me to figure out how to handle it.
				panic(fmt.Errorf("dup key=%v in tree=%v", item.Key, root.TreeID))
			}
			cacheEntry.Items.Insert(oldRebuiltTreeValue{
				Path:     bt.arena.Deflate(path),
				Key:      item.Key,
				ItemSize: item.BodySize,
			})
			if walked != nil {
				*walked = append(*walked, item.Key)
			}
			return nil
		},
	}

	btrfstree.TreeOperatorImpl{NodeSource: bt.inner}.RawTreeWalk(bt.ctx, root, errHandle, cbs)
}

func (bt *OldRebuiltForrest) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (btrfstree.Item, error) {
	return bt.TreeSearch(treeID, btrfstree.SearchExactKey(key))
}

func (bt *OldRebuiltForrest) addErrs(tree oldRebuiltTree, fn func(btrfsprim.Key, uint32) int, err error) error {
	var errs derror.MultiError
	tree.Errors.Subrange(
		func(k btrfsprim.Key) int { return fn(k, 0) },
		func(v oldRebuiltTreeError) bool {
			path := bt.arena.Inflate(v.Path)
			minKey := path.Node(-1).ToKey
			maxKey := path.Node(-1).ToMaxKey
			errs = append(errs, fmt.Errorf("keys %v-%v: %w", minKey, maxKey, v.Err))
			return true
		})
	if len(errs) == 0 {
		return err
	}
	if err != nil {
		errs = append(errs, err)
	}
	return errs
}

func (bt *OldRebuiltForrest) TreeSearch(treeID btrfsprim.ObjID, searcher btrfstree.TreeSearcher) (btrfstree.Item, error) {
	tree := bt.RebuiltTree(treeID)
	if tree.RootErr != nil {
		return btrfstree.Item{}, tree.RootErr
	}

	indexItem := tree.Items.Search(func(indexItem oldRebuiltTreeValue) int {
		return searcher.Search(indexItem.Key, indexItem.ItemSize)
	})
	if indexItem == nil {
		return btrfstree.Item{}, fmt.Errorf("item with %s: %w", searcher, bt.addErrs(tree, searcher.Search, btrfstree.ErrNoItem))
	}

	itemPath := bt.arena.Inflate(indexItem.Value.Path)
	node, err := bt.inner.ReadNode(itemPath.Parent())
	defer btrfstree.FreeNodeRef(node)
	if err != nil {
		return btrfstree.Item{}, fmt.Errorf("item with %s: %w", searcher, bt.addErrs(tree, searcher.Search, err))
	}

	item := node.Data.BodyLeaf[itemPath.Node(-1).FromItemSlot]
	item.Body = item.Body.CloneItem()

	// Since we were only asked to return 1 item, it isn't
	// necessary to augment this `nil` with bt.addErrs().
	return item, nil
}

func (bt *OldRebuiltForrest) TreeSearchAll(treeID btrfsprim.ObjID, searcher btrfstree.TreeSearcher) ([]btrfstree.Item, error) {
	tree := bt.RebuiltTree(treeID)
	if tree.RootErr != nil {
		return nil, tree.RootErr
	}

	var indexItems []oldRebuiltTreeValue
	tree.Items.Subrange(
		func(indexItem oldRebuiltTreeValue) int {
			return searcher.Search(indexItem.Key, indexItem.ItemSize)
		},
		func(node *containers.RBNode[oldRebuiltTreeValue]) bool {
			indexItems = append(indexItems, node.Value)
			return true
		})
	if len(indexItems) == 0 {
		return nil, fmt.Errorf("items with %s: %w", searcher, bt.addErrs(tree, searcher.Search, btrfstree.ErrNoItem))
	}

	ret := make([]btrfstree.Item, len(indexItems))
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]
	for i := range indexItems {
		itemPath := bt.arena.Inflate(indexItems[i].Path)
		if node == nil || node.Addr != itemPath.Node(-2).ToNodeAddr {
			var err error
			btrfstree.FreeNodeRef(node)
			node, err = bt.inner.ReadNode(itemPath.Parent())
			if err != nil {
				btrfstree.FreeNodeRef(node)
				return nil, fmt.Errorf("items with %s: %w", searcher, bt.addErrs(tree, searcher.Search, err))
			}
		}
		ret[i] = node.Data.BodyLeaf[itemPath.Node(-1).FromItemSlot]
		ret[i].Body = ret[i].Body.CloneItem()
	}
	btrfstree.FreeNodeRef(node)

	err := bt.addErrs(tree, searcher.Search, nil)
	if err != nil {
		err = fmt.Errorf("items with %s: %w", searcher, err)
	}
	return ret, err
}

func (bt *OldRebuiltForrest) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*btrfstree.TreeError), cbs btrfstree.TreeWalkHandler) {
	tree := bt.RebuiltTree(treeID)
	if tree.RootErr != nil {
		errHandle(&btrfstree.TreeError{
			Path: btrfstree.TreePath{{
				FromTree: treeID,
				ToMaxKey: btrfsprim.MaxKey,
			}},
			Err: tree.RootErr,
		})
		return
	}
	if cbs.Item == nil {
		return
	}
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]
	tree.Items.Range(func(indexItem *containers.RBNode[oldRebuiltTreeValue]) bool {
		if ctx.Err() != nil {
			return false
		}
		if bt.ctx.Err() != nil {
			return false
		}
		itemPath := bt.arena.Inflate(indexItem.Value.Path)
		if node == nil || node.Addr != itemPath.Node(-2).ToNodeAddr {
			var err error
			btrfstree.FreeNodeRef(node)
			node, err = bt.inner.ReadNode(itemPath.Parent())
			if err != nil {
				btrfstree.FreeNodeRef(node)
				errHandle(&btrfstree.TreeError{Path: itemPath, Err: err})
				return true
			}
		}
		item := node.Data.BodyLeaf[itemPath.Node(-1).FromItemSlot]
		if err := cbs.Item(itemPath, item); err != nil {
			errHandle(&btrfstree.TreeError{Path: itemPath, Err: err})
		}
		return true
	})
	btrfstree.FreeNodeRef(node)
}

func (bt *OldRebuiltForrest) Superblock() (*btrfstree.Superblock, error) {
	return bt.inner.Superblock()
}

func (bt *OldRebuiltForrest) ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return bt.inner.ReadAt(p, off)
}

func (bt *OldRebuiltForrest) Augment(treeID btrfsprim.ObjID, nodeAddr btrfsvol.LogicalAddr) ([]btrfsprim.Key, error) {
	sb, err := bt.Superblock()
	if err != nil {
		return nil, err
	}
	tree := bt.RebuiltTree(treeID)
	if tree.RootErr != nil {
		return nil, tree.RootErr
	}
	nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](bt.inner, *sb, nodeAddr, btrfstree.NodeExpectations{})
	defer btrfstree.FreeNodeRef(nodeRef)
	if err != nil {
		return nil, err
	}
	var ret []btrfsprim.Key
	bt.rawTreeWalk(btrfstree.TreeRoot{
		TreeID:     treeID,
		RootNode:   nodeAddr,
		Level:      nodeRef.Data.Head.Level,
		Generation: nodeRef.Data.Head.Generation,
	}, tree, &ret)
	return ret, nil
}
