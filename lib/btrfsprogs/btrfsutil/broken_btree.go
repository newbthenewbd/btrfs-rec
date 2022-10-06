// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	iofs "io/fs"
	"math"
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

var maxKey = btrfsprim.Key{
	ObjectID: math.MaxUint64,
	ItemType: math.MaxUint8,
	Offset:   math.MaxUint64,
}

type treeIndex struct {
	TreeRootErr error
	Items       *containers.RBTree[btrfsprim.Key, treeIndexValue]
	Errors      *containers.IntervalTree[btrfsprim.Key, treeIndexError]
}

type treeIndexError struct {
	Path SkinnyPath
	Err  error
}

type treeIndexValue struct {
	Path     SkinnyPath
	Key      btrfsprim.Key
	ItemSize uint32
}

func newTreeIndex(arena *SkinnyPathArena) treeIndex {
	return treeIndex{
		Items: &containers.RBTree[btrfsprim.Key, treeIndexValue]{
			KeyFn: func(iv treeIndexValue) btrfsprim.Key {
				return iv.Key
			},
		},
		Errors: &containers.IntervalTree[btrfsprim.Key, treeIndexError]{
			MinFn: func(err treeIndexError) btrfsprim.Key {
				return arena.Inflate(err.Path).Node(-1).ToKey
			},
			MaxFn: func(err treeIndexError) btrfsprim.Key {
				return arena.Inflate(err.Path).Node(-1).ToMaxKey
			},
		},
	}
}

type brokenTrees struct {
	ctx   context.Context
	inner *btrfs.FS

	arena *SkinnyPathArena

	// btrfsprim.ROOT_TREE_OBJECTID
	rootTreeMu    sync.Mutex
	rootTreeIndex *treeIndex
	// for all other trees
	treeMu      sync.Mutex
	treeIndexes map[btrfsprim.ObjID]treeIndex
}

var _ btrfstree.TreeOperator = (*brokenTrees)(nil)

// NewBrokenTrees wraps a *btrfs.FS to support looking up information
// from broken trees.
//
// Of the btrfs.FS.Tree{Verb}Trees methods:
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
// NewBrokenTrees attempts to remedy these deficiencies by using
// .TreeWalk to build an out-of-FS index of all of the items in the
// tree, and re-implements TreeLookup, TreeSearch, and TreeSearchAll
// using that index.
func NewBrokenTrees(ctx context.Context, inner *btrfs.FS) interface {
	btrfstree.TreeOperator
	Superblock() (*btrfstree.Superblock, error)
	ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error)
} {
	return &brokenTrees{
		ctx:   ctx,
		inner: inner,
	}
}

func (bt *brokenTrees) treeIndex(treeID btrfsprim.ObjID) treeIndex {
	var treeRoot *btrfstree.TreeRoot
	var sb *btrfstree.Superblock
	var err error
	if treeID == btrfsprim.ROOT_TREE_OBJECTID {
		bt.rootTreeMu.Lock()
		defer bt.rootTreeMu.Unlock()
		if bt.rootTreeIndex != nil {
			return *bt.rootTreeIndex
		}
		sb, err = bt.inner.Superblock()
		if err == nil {
			treeRoot, err = btrfstree.LookupTreeRoot(bt.inner, *sb, treeID)
		}
	} else {
		bt.treeMu.Lock()
		defer bt.treeMu.Unlock()
		if bt.treeIndexes == nil {
			bt.treeIndexes = make(map[btrfsprim.ObjID]treeIndex)
		}
		if cacheEntry, exists := bt.treeIndexes[treeID]; exists {
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
	cacheEntry := newTreeIndex(bt.arena)
	if err != nil {
		cacheEntry.TreeRootErr = err
	} else {
		dlog.Infof(bt.ctx, "indexing tree %v...", treeID)
		btrfstree.TreeOperatorImpl{NodeSource: bt.inner}.RawTreeWalk(
			bt.ctx,
			*treeRoot,
			func(err *btrfstree.TreeError) {
				if len(err.Path) > 0 && err.Path.Node(-1).ToNodeAddr == 0 {
					// This is a panic because on the filesystems I'm working with it more likely
					// indicates a bug in my item parser than a problem with the filesystem.
					panic(fmt.Errorf("TODO: error parsing item: %w", err))
				}
				cacheEntry.Errors.Insert(treeIndexError{
					Path: bt.arena.Deflate(err.Path),
					Err:  err.Err,
				})
			},
			btrfstree.TreeWalkHandler{
				Item: func(path btrfstree.TreePath, item btrfstree.Item) error {
					if cacheEntry.Items.Lookup(item.Key) != nil {
						// This is a panic because I'm not really sure what the best way to
						// handle this is, and so if this happens I want the program to crash
						// and force me to figure out how to handle it.
						panic(fmt.Errorf("dup key=%v in tree=%v", item.Key, treeID))
					}
					cacheEntry.Items.Insert(treeIndexValue{
						Path:     bt.arena.Deflate(path),
						Key:      item.Key,
						ItemSize: item.BodySize,
					})
					return nil
				},
			},
		)
		dlog.Infof(bt.ctx, "... done indexing tree %v", treeID)
	}
	if treeID == btrfsprim.ROOT_TREE_OBJECTID {
		bt.rootTreeIndex = &cacheEntry
	} else {
		bt.treeIndexes[treeID] = cacheEntry
	}
	return cacheEntry
}

func (bt *brokenTrees) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (btrfstree.Item, error) {
	item, err := bt.TreeSearch(treeID, btrfstree.KeySearch(key.Cmp))
	if err != nil {
		err = fmt.Errorf("item with key=%v: %w", key, err)
	}
	return item, err
}

func (bt *brokenTrees) TreeSearch(treeID btrfsprim.ObjID, fn func(btrfsprim.Key, uint32) int) (btrfstree.Item, error) {
	index := bt.treeIndex(treeID)
	if index.TreeRootErr != nil {
		return btrfstree.Item{}, index.TreeRootErr
	}
	indexItem := index.Items.Search(func(indexItem treeIndexValue) int {
		return fn(indexItem.Key, indexItem.ItemSize)
	})
	if indexItem == nil {
		return btrfstree.Item{}, iofs.ErrNotExist
	}

	itemPath := bt.arena.Inflate(indexItem.Value.Path)
	node, err := bt.inner.ReadNode(itemPath.Parent())
	if err != nil {
		return btrfstree.Item{}, err
	}

	item := node.Data.BodyLeaf[itemPath.Node(-1).FromItemIdx]

	if errs := index.Errors.SearchAll(func(k btrfsprim.Key) int { return fn(k, 0) }); len(errs) > 0 {
		err := make(derror.MultiError, len(errs))
		for i := range errs {
			err[i] = &btrfstree.TreeError{
				Path: bt.arena.Inflate(errs[i].Path),
				Err:  errs[i].Err,
			}
		}
		return item, err
	}

	return item, nil
}

func (bt *brokenTrees) TreeSearchAll(treeID btrfsprim.ObjID, fn func(btrfsprim.Key, uint32) int) ([]btrfstree.Item, error) {
	index := bt.treeIndex(treeID)
	if index.TreeRootErr != nil {
		return nil, index.TreeRootErr
	}
	indexItems := index.Items.SearchRange(func(indexItem treeIndexValue) int {
		return fn(indexItem.Key, indexItem.ItemSize)
	})
	if len(indexItems) == 0 {
		return nil, iofs.ErrNotExist
	}

	ret := make([]btrfstree.Item, len(indexItems))
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]
	for i := range indexItems {
		itemPath := bt.arena.Inflate(indexItems[i].Path)
		if node == nil || node.Addr != itemPath.Node(-2).ToNodeAddr {
			var err error
			node, err = bt.inner.ReadNode(itemPath.Parent())
			if err != nil {
				return nil, err
			}
		}
		ret[i] = node.Data.BodyLeaf[itemPath.Node(-1).FromItemIdx]
	}

	if errs := index.Errors.SearchAll(func(k btrfsprim.Key) int { return fn(k, 0) }); len(errs) > 0 {
		err := make(derror.MultiError, len(errs))
		for i := range errs {
			err[i] = &btrfstree.TreeError{
				Path: bt.arena.Inflate(errs[i].Path),
				Err:  errs[i].Err,
			}
		}
		return ret, err
	}

	return ret, nil
}

func (bt *brokenTrees) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*btrfstree.TreeError), cbs btrfstree.TreeWalkHandler) {
	index := bt.treeIndex(treeID)
	if index.TreeRootErr != nil {
		errHandle(&btrfstree.TreeError{
			Path: btrfstree.TreePath{{
				FromTree: treeID,
				ToMaxKey: maxKey,
			}},
			Err: index.TreeRootErr,
		})
		return
	}
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]
	_ = index.Items.Walk(func(indexItem *containers.RBNode[treeIndexValue]) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if bt.ctx.Err() != nil {
			return bt.ctx.Err()
		}
		if cbs.Item != nil {
			itemPath := bt.arena.Inflate(indexItem.Value.Path)
			if node == nil || node.Addr != itemPath.Node(-2).ToNodeAddr {
				var err error
				node, err = bt.inner.ReadNode(itemPath.Parent())
				if err != nil {
					errHandle(&btrfstree.TreeError{Path: itemPath, Err: err})
					return nil
				}
			}
			item := node.Data.BodyLeaf[itemPath.Node(-1).FromItemIdx]
			if err := cbs.Item(itemPath, item); err != nil {
				errHandle(&btrfstree.TreeError{Path: itemPath, Err: err})
			}
		}
		return nil
	})
}

func (bt *brokenTrees) Superblock() (*btrfstree.Superblock, error) {
	return bt.inner.Superblock()
}

func (bt *brokenTrees) ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return bt.inner.ReadAt(p, off)
}
