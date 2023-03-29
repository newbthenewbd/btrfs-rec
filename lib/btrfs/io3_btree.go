// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

// This file is ordered from low-level to high-level.

// btrfstree.NodeFile //////////////////////////////////////////////////////////

type treeInfo struct {
	UUID       btrfsprim.UUID
	ParentUUID btrfsprim.UUID
	ParentGen  btrfsprim.Generation
}

func (fs *FS) populateTreeUUIDs(ctx context.Context) {
	if fs.cacheObjID2All != nil && fs.cacheUUID2ObjID != nil {
		return
	}
	fs.cacheObjID2All = make(map[btrfsprim.ObjID]treeInfo)
	fs.cacheUUID2ObjID = make(map[btrfsprim.UUID]btrfsprim.ObjID)
	fs.TreeWalk(ctx, btrfsprim.ROOT_TREE_OBJECTID,
		func(err *btrfstree.TreeError) {
			// do nothing
		},
		btrfstree.TreeWalkHandler{
			Item: func(_ btrfstree.Path, item btrfstree.Item) {
				itemBody, ok := item.Body.(*btrfsitem.Root)
				if !ok {
					return
				}
				fs.cacheObjID2All[item.Key.ObjectID] = treeInfo{
					UUID:       itemBody.UUID,
					ParentUUID: itemBody.ParentUUID,
					ParentGen:  btrfsprim.Generation(item.Key.Offset),
				}
				fs.cacheUUID2ObjID[itemBody.UUID] = item.Key.ObjectID
			},
		},
	)
}

// ParentTree implements btrfstree.NodeFile.
func (fs *FS) ParentTree(tree btrfsprim.ObjID) (btrfsprim.ObjID, btrfsprim.Generation, bool) {
	if tree < btrfsprim.FIRST_FREE_OBJECTID || tree > btrfsprim.LAST_FREE_OBJECTID {
		// no parent
		return 0, 0, true
	}
	fs.populateTreeUUIDs(context.TODO())

	all, ok := fs.cacheObjID2All[tree]
	if !ok {
		// could not look up parent info
		return 0, 0, false
	}
	if all.ParentUUID == (btrfsprim.UUID{}) {
		// no parent
		return 0, 0, true
	}
	parentObjID, ok := fs.cacheUUID2ObjID[all.ParentUUID]
	if !ok {
		// could not look up parent info
		return 0, 0, false
	}
	return parentObjID, all.ParentGen, true
}

var _ btrfstree.NodeFile = (*FS)(nil)

// btrfstree.NodeSource ////////////////////////////////////////////////////////

// ReadNode implements btrfstree.NodeSource.
func (fs *FS) ReadNode(_ context.Context, addr btrfsvol.LogicalAddr, exp btrfstree.NodeExpectations) (*btrfstree.Node, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, fmt.Errorf("btrfs.FS.ReadNode: %w", err)
	}
	return btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, addr, exp)
}

var _ btrfstree.NodeSource = (*FS)(nil)

// btrfstree.TreeOperator //////////////////////////////////////////////////////

// TreeWalk implements btrfstree.TreeOperator.
func (fs *FS) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*btrfstree.TreeError), cbs btrfstree.TreeWalkHandler) {
	btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeWalk(ctx, treeID, errHandle, cbs)
}

// TreeLookup implements btrfstree.TreeOperator.
func (fs *FS) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeLookup(treeID, key)
}

// TreeSearch implements btrfstree.TreeOperator.
func (fs *FS) TreeSearch(treeID btrfsprim.ObjID, searcher btrfstree.TreeSearcher) (btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeSearch(treeID, searcher)
}

// TreeSearchAll implements btrfstree.TreeOperator.
func (fs *FS) TreeSearchAll(treeID btrfsprim.ObjID, searcher btrfstree.TreeSearcher) ([]btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeSearchAll(treeID, searcher)
}

var _ btrfstree.TreeOperator = (*FS)(nil)
