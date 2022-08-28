// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

var _ Trees = (*FS)(nil)

func (fs *FS) populateTreeUUIDs(ctx context.Context) {
	if fs.cacheObjID2UUID == nil || fs.cacheUUID2ObjID == nil || fs.cacheTreeParent == nil {
		fs.cacheObjID2UUID = make(map[ObjID]UUID)
		fs.cacheUUID2ObjID = make(map[UUID]ObjID)
		fs.cacheTreeParent = make(map[ObjID]UUID)
		fs.TreeWalk(ctx, ROOT_TREE_OBJECTID,
			func(err *TreeError) {
				// do nothing
			},
			TreeWalkHandler{
				Item: func(_ TreePath, item Item) error {
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

func (fs *FS) ReadNode(path TreePath) (*diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, fmt.Errorf("btrfs.FS.ReadNode: %w", err)
	}

	potentialOwners := []ObjID{
		path.Node(-1).FromTree,
	}
	if potentialOwners[0] >= FIRST_FREE_OBJECTID {
		ctx := context.TODO()
		fs.populateTreeUUIDs(ctx)
		for potentialOwners[len(potentialOwners)-1] >= FIRST_FREE_OBJECTID {
			objID := potentialOwners[len(potentialOwners)-1]
			parentUUID := fs.cacheTreeParent[objID]
			if parentUUID == (UUID{}) {
				break
			}
			parentObjID, ok := fs.cacheUUID2ObjID[parentUUID]
			if !ok {
				break
			}
			potentialOwners = append(potentialOwners, parentObjID)
		}
	}

	return ReadNode[btrfsvol.LogicalAddr](fs, *sb, path.Node(-1).ToNodeAddr, NodeExpectations{
		LAddr:         containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: path.Node(-1).ToNodeAddr},
		Level:         containers.Optional[uint8]{OK: true, Val: path.Node(-1).ToNodeLevel},
		MaxGeneration: containers.Optional[Generation]{OK: true, Val: path.Node(-1).FromGeneration},
		Owner:         potentialOwners,
	})
}
