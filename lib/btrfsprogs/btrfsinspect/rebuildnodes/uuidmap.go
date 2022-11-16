// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type uuidMap struct {
	ObjID2UUID map[btrfsprim.ObjID]btrfsprim.UUID
	UUID2ObjID map[btrfsprim.UUID]btrfsprim.ObjID
	TreeParent map[btrfsprim.ObjID]btrfsprim.UUID

	SeenTrees containers.Set[btrfsprim.ObjID]
}

func (m uuidMap) missingRootItems() containers.Set[btrfsprim.ObjID] {
	missing := make(containers.Set[btrfsprim.ObjID])
	for treeID := range m.SeenTrees {
		if _, ok := m.ObjID2UUID[treeID]; !ok && treeID != btrfsprim.ROOT_TREE_OBJECTID {
			missing.Insert(treeID)
			continue
		}
		if _, ok := m.TreeParent[treeID]; !ok && treeID >= btrfsprim.FIRST_FREE_OBJECTID && treeID <= btrfsprim.LAST_FREE_OBJECTID {
			missing.Insert(treeID)
		}
	}
	return missing
}

// ParentTree implements btrfstree.NodeFile.
func (m uuidMap) ParentTree(tree btrfsprim.ObjID) (btrfsprim.ObjID, bool) {
	if tree < btrfsprim.FIRST_FREE_OBJECTID || tree > btrfsprim.LAST_FREE_OBJECTID {
		// no parent
		return 0, true
	}
	parentUUID, ok := m.TreeParent[tree]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	if parentUUID == (btrfsprim.UUID{}) {
		// no parent
		return 0, true
	}
	parentObjID, ok := m.UUID2ObjID[parentUUID]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	return parentObjID, true
}
