// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
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
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func (fs *FS) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*btrfstree.TreeError), cbs btrfstree.TreeWalkHandler) {
	btrfstree.TreesImpl{NodeSource: fs}.TreeWalk(ctx, treeID, errHandle, cbs)
}
func (fs *FS) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (btrfstree.Item, error) {
	return btrfstree.TreesImpl{NodeSource: fs}.TreeLookup(treeID, key)
}
func (fs *FS) TreeSearch(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) (btrfstree.Item, error) {
	return btrfstree.TreesImpl{NodeSource: fs}.TreeSearch(treeID, fn)
}
func (fs *FS) TreeSearchAll(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) ([]btrfstree.Item, error) {
	return btrfstree.TreesImpl{NodeSource: fs}.TreeSearchAll(treeID, fn)
}

var _ btrfstree.Trees = (*FS)(nil)

func (fs *FS) populateTreeUUIDs(ctx context.Context) {
	if fs.cacheObjID2UUID == nil || fs.cacheUUID2ObjID == nil || fs.cacheTreeParent == nil {
		fs.cacheObjID2UUID = make(map[btrfsprim.ObjID]btrfsprim.UUID)
		fs.cacheUUID2ObjID = make(map[btrfsprim.UUID]btrfsprim.ObjID)
		fs.cacheTreeParent = make(map[btrfsprim.ObjID]btrfsprim.UUID)
		fs.TreeWalk(ctx, btrfsprim.ROOT_TREE_OBJECTID,
			func(err *btrfstree.TreeError) {
				// do nothing
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

func (fs *FS) ReadNode(path btrfstree.TreePath) (*diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, fmt.Errorf("btrfs.FS.ReadNode: %w", err)
	}

	potentialOwners := []btrfsprim.ObjID{
		path.Node(-1).FromTree,
	}
	if potentialOwners[0] >= btrfsprim.FIRST_FREE_OBJECTID {
		ctx := context.TODO()
		fs.populateTreeUUIDs(ctx)
		for potentialOwners[len(potentialOwners)-1] >= btrfsprim.FIRST_FREE_OBJECTID {
			objID := potentialOwners[len(potentialOwners)-1]
			parentUUID := fs.cacheTreeParent[objID]
			if parentUUID == (btrfsprim.UUID{}) {
				break
			}
			parentObjID, ok := fs.cacheUUID2ObjID[parentUUID]
			if !ok {
				break
			}
			potentialOwners = append(potentialOwners, parentObjID)
		}
	}

	return btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, path.Node(-1).ToNodeAddr, btrfstree.NodeExpectations{
		LAddr:         containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: path.Node(-1).ToNodeAddr},
		Level:         containers.Optional[uint8]{OK: true, Val: path.Node(-1).ToNodeLevel},
		MaxGeneration: containers.Optional[btrfsprim.Generation]{OK: true, Val: path.Node(-1).FromGeneration},
		Owner:         potentialOwners,
	})
}
