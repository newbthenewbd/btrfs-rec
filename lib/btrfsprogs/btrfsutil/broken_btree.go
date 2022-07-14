// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	iofs "io/fs"
	"sync"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type indexItem struct {
	Key  btrfs.Key
	Path btrfs.TreePath
}

type cachedIndex struct {
	Index *containers.RBTree[btrfs.Key, indexItem]
	Err   error
}

type brokenTrees struct {
	ctx   context.Context
	inner *btrfs.FS

	// btrfs.ROOT_TREE_OBJECTID
	rootTreeMu    sync.Mutex
	rootTreeIndex *cachedIndex
	// for all other trees
	treeMu      sync.Mutex
	treeIndexes map[btrfs.ObjID]cachedIndex
}

var _ btrfs.Trees = (*brokenTrees)(nil)

// NewBrokenTrees wraps a *btrfs.FS to support looking up information
// from broken trees.
//
// Of the btrfs.FS.Tree{Verb}Trees methods:
//
//  - TreeWalk works on broken trees
//  - TreeLookup relies on the tree being properly ordered (which a
//    broken tree might not be).
//  - TreeSearch relies on the tree being properly ordered (which a
//    broken tree might not be).
//  - TreeSearchAll relies on the tree being properly ordered (which a
//    broken tree might not be), and a bad node may cause it to not
//    return a truncated list of results.
//
// NewBrokenTrees attempts to remedy these deficiencies by using
// .TreeWalk to build an out-of-FS index of all of the items in the
// tree, and re-implements TreeLookup, TreeSearch, and TreeSearchAll
// using that index.
func NewBrokenTrees(ctx context.Context, inner *btrfs.FS) btrfs.Trees {
	return &brokenTrees{
		ctx:   ctx,
		inner: inner,
	}
}

func (bt *brokenTrees) treeIndex(treeID btrfs.ObjID) (*containers.RBTree[btrfs.Key, indexItem], error) {
	var treeRoot *btrfs.TreeRoot
	var err error
	if treeID == btrfs.ROOT_TREE_OBJECTID {
		bt.rootTreeMu.Lock()
		defer bt.rootTreeMu.Unlock()
		if bt.rootTreeIndex != nil {
			return bt.rootTreeIndex.Index, bt.rootTreeIndex.Err
		}
		treeRoot, err = btrfs.LookupTreeRoot(bt.inner, treeID)
	} else {
		bt.treeMu.Lock()
		defer bt.treeMu.Unlock()
		if bt.treeIndexes == nil {
			bt.treeIndexes = make(map[btrfs.ObjID]cachedIndex)
		}
		if cacheEntry, exists := bt.treeIndexes[treeID]; exists {
			return cacheEntry.Index, cacheEntry.Err
		}
		treeRoot, err = btrfs.LookupTreeRoot(bt, treeID)
	}
	var cacheEntry cachedIndex
	if err != nil {
		cacheEntry.Err = err
	} else {
		cacheEntry.Index = &containers.RBTree[btrfs.Key, indexItem]{
			KeyFn: func(item indexItem) btrfs.Key {
				return item.Key
			},
		}
		dlog.Infof(bt.ctx, "indexing tree %v...", treeID)
		bt.inner.RawTreeWalk(
			bt.ctx,
			*treeRoot,
			func(err *btrfs.TreeError) {
				dlog.Error(bt.ctx, err)
			},
			btrfs.TreeWalkHandler{
				Item: func(path btrfs.TreePath, item btrfs.Item) error {
					if cacheEntry.Index.Lookup(item.Key) != nil {
						panic(fmt.Errorf("dup key=%v in tree=%v", item.Key, treeID))
					}
					cacheEntry.Index.Insert(indexItem{
						Path: path.DeepCopy(),
						Key:  item.Key,
					})
					return nil
				},
			},
		)
		dlog.Infof(bt.ctx, "... done indexing tree %v", treeID)
	}
	if treeID == btrfs.ROOT_TREE_OBJECTID {
		bt.rootTreeIndex = &cacheEntry
	} else {
		bt.treeIndexes[treeID] = cacheEntry
	}
	return cacheEntry.Index, cacheEntry.Err
}

func (bt *brokenTrees) TreeLookup(treeID btrfs.ObjID, key btrfs.Key) (btrfs.Item, error) {
	item, err := bt.TreeSearch(treeID, key.Cmp)
	if err != nil {
		err = fmt.Errorf("item with key=%v: %w", key, err)
	}
	return item, err
}

func (bt *brokenTrees) TreeSearch(treeID btrfs.ObjID, fn func(btrfs.Key) int) (btrfs.Item, error) {
	index, err := bt.treeIndex(treeID)
	if err != nil {
		return btrfs.Item{}, err
	}
	indexItem := index.Search(func(indexItem indexItem) int {
		return fn(indexItem.Key)
	})
	if indexItem == nil {
		return btrfs.Item{}, iofs.ErrNotExist
	}

	node, err := bt.inner.ReadNode(indexItem.Value.Path.Node(-2).NodeAddr)
	if err != nil {
		return btrfs.Item{}, err
	}

	item := node.Data.BodyLeaf[indexItem.Value.Path.Node(-1).ItemIdx]

	return item, nil
}

func (bt *brokenTrees) TreeSearchAll(treeID btrfs.ObjID, fn func(btrfs.Key) int) ([]btrfs.Item, error) {
	index, err := bt.treeIndex(treeID)
	if err != nil {
		return nil, err
	}
	indexItems := index.SearchRange(func(indexItem indexItem) int {
		return fn(indexItem.Key)
	})
	if len(indexItems) == 0 {
		return nil, iofs.ErrNotExist
	}

	ret := make([]btrfs.Item, len(indexItems))
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]
	for i := range indexItems {
		if node == nil || node.Addr != indexItems[i].Path.Node(-2).NodeAddr {
			var err error
			node, err = bt.inner.ReadNode(indexItems[i].Path.Node(-2).NodeAddr)
			if err != nil {
				return nil, err
			}
		}
		ret[i] = node.Data.BodyLeaf[indexItems[i].Path.Node(-1).ItemIdx]
	}
	return ret, nil
}

func (bt *brokenTrees) TreeWalk(ctx context.Context, treeID btrfs.ObjID, errHandle func(*btrfs.TreeError), cbs btrfs.TreeWalkHandler) {
	index, err := bt.treeIndex(treeID)
	if err != nil {
		errHandle(&btrfs.TreeError{
			Path: btrfs.TreePath{
				TreeID: treeID,
			},
			Err: err,
		})
		return
	}
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]
	_ = index.Walk(func(indexItem *containers.RBNode[indexItem]) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if bt.ctx.Err() != nil {
			return bt.ctx.Err()
		}
		if cbs.Item != nil {
			if node == nil || node.Addr != indexItem.Value.Path.Node(-2).NodeAddr {
				var err error
				node, err = bt.inner.ReadNode(indexItem.Value.Path.Node(-2).NodeAddr)
				if err != nil {
					errHandle(&btrfs.TreeError{Path: indexItem.Value.Path, Err: err})
					return nil
				}
			}
			item := node.Data.BodyLeaf[indexItem.Value.Path.Node(-1).ItemIdx]
			if err := cbs.Item(indexItem.Value.Path, item); err != nil {
				errHandle(&btrfs.TreeError{Path: indexItem.Value.Path, Err: err})
			}
		}
		return nil
	})
}

func (bt *brokenTrees) Superblock() (*btrfs.Superblock, error) {
	return bt.inner.Superblock()
}

func (bt *brokenTrees) ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return bt.inner.ReadAt(p, off)
}
