// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func (fs *FS) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*btrfstree.TreeError), cbs btrfstree.TreeWalkHandler) {
	btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeWalk(ctx, treeID, errHandle, cbs)
}
func (fs *FS) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeLookup(treeID, key)
}
func (fs *FS) TreeSearch(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) (btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeSearch(treeID, fn)
}
func (fs *FS) TreeSearchAll(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) ([]btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeSearchAll(treeID, fn)
}

var _ btrfstree.TreeOperator = (*FS)(nil)

func (fs *FS) populateTreeUUIDs(ctx context.Context) {
	if fs.cacheObjID2UUID == nil || fs.cacheUUID2ObjID == nil || fs.cacheTreeParent == nil {
		fs.cacheObjID2UUID = make(map[btrfsprim.ObjID]btrfsprim.UUID)
		fs.cacheUUID2ObjID = make(map[btrfsprim.UUID]btrfsprim.ObjID)
		fs.cacheTreeParent = make(map[btrfsprim.ObjID]btrfsprim.UUID)
		fs.TreeWalk(ctx, btrfsprim.ROOT_TREE_OBJECTID,
			func(err *btrfstree.TreeError) {
				// do nothing
				dlog.Errorf(ctx, "dbg err: %v", err)
			},
			btrfstree.TreeWalkHandler{
				Item: func(_ btrfstree.TreePath, item btrfstree.Item) error {
					itemBody, ok := item.Body.(btrfsitem.Root)
					if !ok {
						return nil
					}
					fs.cacheObjID2UUID[item.Key.ObjectID] = itemBody.UUID
					fs.cacheTreeParent[item.Key.ObjectID] = itemBody.ParentUUID
					fs.cacheUUID2ObjID[itemBody.UUID] = item.Key.ObjectID
					return nil
				},
			},
		)
	}
}

func (fs *FS) ParentTree(tree btrfsprim.ObjID) (btrfsprim.ObjID, bool) {
	if tree < btrfsprim.FIRST_FREE_OBJECTID || tree > btrfsprim.LAST_FREE_OBJECTID {
		// no parent
		return 0, true
	}
	fs.populateTreeUUIDs(context.TODO())
	parentUUID, ok := fs.cacheTreeParent[tree]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	if parentUUID == (btrfsprim.UUID{}) {
		// no parent
		return 0, true
	}
	parentObjID, ok := fs.cacheUUID2ObjID[parentUUID]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	return parentObjID, true
}

func (fs *FS) ReadNode(path btrfstree.TreePath) (*diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], error) {
	return btrfstree.FSReadNode(fs, path)
}
