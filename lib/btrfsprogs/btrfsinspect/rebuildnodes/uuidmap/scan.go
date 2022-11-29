// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package uuidmap

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func maybeSet[K, V comparable](name string, m map[K]V, k K, v V) error {
	if other, conflict := m[k]; conflict && other != v {
		return fmt.Errorf("conflict: %s %v can't have both %v and %v", name, k, other, v)
	}
	m[k] = v
	return nil
}

func New() *UUIDMap {
	ret := &UUIDMap{
		ObjID2UUID: make(map[btrfsprim.ObjID]btrfsprim.UUID),
		UUID2ObjID: make(map[btrfsprim.UUID]btrfsprim.ObjID),
		TreeParent: make(map[btrfsprim.ObjID]btrfsprim.UUID),

		SeenTrees: make(containers.Set[btrfsprim.ObjID]),
	}

	// These 4 trees are mentioned directly in the superblock, so
	// they are always seen.
	ret.SeenTrees.Insert(btrfsprim.ROOT_TREE_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.CHUNK_TREE_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.TREE_LOG_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.BLOCK_GROUP_TREE_OBJECTID)

	return ret
}

func (m *UUIDMap) InsertNode(nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
	for _, item := range nodeRef.Data.BodyLeaf {
		switch itemBody := item.Body.(type) {
		case btrfsitem.Root:
			if err := maybeSet("ObjID2UUID", m.ObjID2UUID, item.Key.ObjectID, itemBody.UUID); err != nil {
				return err
			}
			if itemBody.UUID != (btrfsprim.UUID{}) {
				if err := maybeSet("UUID2ObjID", m.UUID2ObjID, itemBody.UUID, item.Key.ObjectID); err != nil {
					return err
				}
			}
			if err := maybeSet("ParentUUID", m.TreeParent, item.Key.ObjectID, itemBody.ParentUUID); err != nil {
				return err
			}
			m.SeenTrees.Insert(item.Key.ObjectID)
		case btrfsitem.UUIDMap:
			uuid := btrfsitem.KeyToUUID(item.Key)
			if err := maybeSet("ObjID2UUID", m.ObjID2UUID, itemBody.ObjID, uuid); err != nil {
				return err
			}
			if err := maybeSet("UUID2ObjID", m.UUID2ObjID, uuid, itemBody.ObjID); err != nil {
				return err
			}
		}
	}
	m.SeenTrees.Insert(nodeRef.Data.Head.Owner)
	return nil
}
