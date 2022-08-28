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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type indexValue struct {
	Key      btrfs.Key
	ItemSize uint32
	Path     btrfs.TreePath
}

var maxKey = btrfs.Key{
	ObjectID: math.MaxUint64,
	ItemType: math.MaxUint8,
	Offset:   math.MaxUint64,
}

func keyMm(key btrfs.Key) btrfs.Key {
	switch {
	case key.Offset > 0:
		key.Offset--
	case key.ItemType > 0:
		key.ItemType--
	case key.ObjectID > 0:
		key.ObjectID--
	}
	return key
}

func span(fs *btrfs.FS, path btrfs.TreePath) (btrfs.Key, btrfs.Key) {
	// tree root error
	if len(path) == 0 {
		return btrfs.Key{}, maxKey
	}

	// item error
	if path.Node(-1).ToNodeAddr == 0 {
		// If we got an item error, then the node is readable
		node, _ := fs.ReadNode(path[:len(path)-1])
		key := node.Data.BodyLeaf[path.Node(-1).FromItemIdx].Key
		return key, key
	}

	// node error
	//
	// assume that path.Node(-1).NodeAddr is not readable, but that path.Node(-2).NodeAddr is.
	if len(path) == 1 {
		return btrfs.Key{}, maxKey
	}
	parentNode, _ := fs.ReadNode(path.Parent())
	low := parentNode.Data.BodyInternal[path.Node(-1).FromItemIdx].Key
	var high btrfs.Key
	if path.Node(-1).FromItemIdx+1 < len(parentNode.Data.BodyInternal) {
		high = keyMm(parentNode.Data.BodyInternal[path.Node(-1).FromItemIdx+1].Key)
	} else {
		parentPath := path.Parent().DeepCopy()
		_, high = span(fs, parentPath)
	}
	return low, high
}

type spanError struct {
	End btrfs.Key
	Err error
}

type cachedIndex struct {
	TreeRootErr error
	Items       *containers.RBTree[btrfs.Key, indexValue]
	Errors      map[int]map[btrfs.Key][]spanError
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
func NewBrokenTrees(ctx context.Context, inner *btrfs.FS) btrfs.Trees {
	return &brokenTrees{
		ctx:   ctx,
		inner: inner,
	}
}

func (bt *brokenTrees) treeIndex(treeID btrfs.ObjID) cachedIndex {
	var treeRoot *btrfs.TreeRoot
	var err error
	if treeID == btrfs.ROOT_TREE_OBJECTID {
		bt.rootTreeMu.Lock()
		defer bt.rootTreeMu.Unlock()
		if bt.rootTreeIndex != nil {
			return *bt.rootTreeIndex
		}
		treeRoot, err = btrfs.LookupTreeRoot(bt.inner, treeID)
	} else {
		bt.treeMu.Lock()
		defer bt.treeMu.Unlock()
		if bt.treeIndexes == nil {
			bt.treeIndexes = make(map[btrfs.ObjID]cachedIndex)
		}
		if cacheEntry, exists := bt.treeIndexes[treeID]; exists {
			return cacheEntry
		}
		treeRoot, err = btrfs.LookupTreeRoot(bt, treeID)
	}
	var cacheEntry cachedIndex
	cacheEntry.Errors = make(map[int]map[btrfs.Key][]spanError)
	if err != nil {
		cacheEntry.TreeRootErr = err
	} else {
		cacheEntry.Items = &containers.RBTree[btrfs.Key, indexValue]{
			KeyFn: func(iv indexValue) btrfs.Key { return iv.Key },
		}
		dlog.Infof(bt.ctx, "indexing tree %v...", treeID)
		bt.inner.RawTreeWalk(
			bt.ctx,
			*treeRoot,
			func(err *btrfs.TreeError) {
				if len(err.Path) > 0 && err.Path.Node(-1).ToNodeAddr == 0 {
					// This is a panic because on the filesystems I'm working with it more likely
					// indicates a bug in my item parser than a problem with the filesystem.
					panic(fmt.Errorf("TODO: error parsing item: %w", err))
				}
				invLvl := len(err.Path)
				lvlErrs, ok := cacheEntry.Errors[invLvl]
				if !ok {
					lvlErrs = make(map[btrfs.Key][]spanError)
					cacheEntry.Errors[invLvl] = lvlErrs
				}
				beg, end := span(bt.inner, err.Path)
				lvlErrs[beg] = append(lvlErrs[beg], spanError{
					End: end,
					Err: err,
				})
			},
			btrfs.TreeWalkHandler{
				Item: func(path btrfs.TreePath, item btrfs.Item) error {
					if cacheEntry.Items.Lookup(item.Key) != nil {
						// This is a panic because I'm not really sure what the best way to
						// handle this is, and so if this happens I want the program to crash
						// and force me to figure out how to handle it.
						panic(fmt.Errorf("dup key=%v in tree=%v", item.Key, treeID))
					}
					cacheEntry.Items.Insert(indexValue{
						Key:      item.Key,
						ItemSize: item.BodySize,
						Path:     path.DeepCopy(),
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
	return cacheEntry
}

func (bt *brokenTrees) TreeLookup(treeID btrfs.ObjID, key btrfs.Key) (btrfs.Item, error) {
	item, err := bt.TreeSearch(treeID, btrfs.KeySearch(key.Cmp))
	if err != nil {
		err = fmt.Errorf("item with key=%v: %w", key, err)
	}
	return item, err
}

func (bt *brokenTrees) TreeSearch(treeID btrfs.ObjID, fn func(btrfs.Key, uint32) int) (btrfs.Item, error) {
	index := bt.treeIndex(treeID)
	if index.TreeRootErr != nil {
		return btrfs.Item{}, index.TreeRootErr
	}
	indexItem := index.Items.Search(func(indexItem indexValue) int {
		return fn(indexItem.Key, indexItem.ItemSize)
	})
	if indexItem == nil {
		return btrfs.Item{}, iofs.ErrNotExist
	}

	node, err := bt.inner.ReadNode(indexItem.Value.Path.Parent())
	if err != nil {
		return btrfs.Item{}, err
	}

	item := node.Data.BodyLeaf[indexItem.Value.Path.Node(-1).FromItemIdx]

	return item, nil
}

func (bt *brokenTrees) TreeSearchAll(treeID btrfs.ObjID, fn func(btrfs.Key, uint32) int) ([]btrfs.Item, error) {
	index := bt.treeIndex(treeID)
	if index.TreeRootErr != nil {
		return nil, index.TreeRootErr
	}
	indexItems := index.Items.SearchRange(func(indexItem indexValue) int {
		return fn(indexItem.Key, indexItem.ItemSize)
	})
	if len(indexItems) == 0 {
		return nil, iofs.ErrNotExist
	}

	ret := make([]btrfs.Item, len(indexItems))
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]
	for i := range indexItems {
		if node == nil || node.Addr != indexItems[i].Path.Node(-2).ToNodeAddr {
			var err error
			node, err = bt.inner.ReadNode(indexItems[i].Path.Parent())
			if err != nil {
				return nil, err
			}
		}
		ret[i] = node.Data.BodyLeaf[indexItems[i].Path.Node(-1).FromItemIdx]
	}

	var errs derror.MultiError
	for _, invLvl := range maps.SortedKeys(index.Errors) {
		for _, beg := range maps.Keys(index.Errors[invLvl]) {
			if fn(beg, math.MaxUint32) < 0 {
				continue
			}
			for _, spanErr := range index.Errors[invLvl][beg] {
				end := spanErr.End
				err := spanErr.Err
				if fn(end, math.MaxUint32) > 0 {
					continue
				}
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return ret, errs
	}
	return ret, nil
}

func (bt *brokenTrees) TreeWalk(ctx context.Context, treeID btrfs.ObjID, errHandle func(*btrfs.TreeError), cbs btrfs.TreeWalkHandler) {
	index := bt.treeIndex(treeID)
	if index.TreeRootErr != nil {
		errHandle(&btrfs.TreeError{
			Path: btrfs.TreePath{{
				FromTree: treeID,
			}},
			Err: index.TreeRootErr,
		})
		return
	}
	var node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]
	_ = index.Items.Walk(func(indexItem *containers.RBNode[indexValue]) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if bt.ctx.Err() != nil {
			return bt.ctx.Err()
		}
		if cbs.Item != nil {
			if node == nil || node.Addr != indexItem.Value.Path.Node(-2).ToNodeAddr {
				var err error
				node, err = bt.inner.ReadNode(indexItem.Value.Path.Parent())
				if err != nil {
					errHandle(&btrfs.TreeError{Path: indexItem.Value.Path, Err: err})
					return nil
				}
			}
			item := node.Data.BodyLeaf[indexItem.Value.Path.Node(-1).FromItemIdx]
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
