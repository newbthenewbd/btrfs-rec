// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsprim

import (
	"fmt"
)

type ObjID uint64

const maxUint64pp = 0x1_00000000_00000000

const (
	// The IDs of the various trees
	ROOT_TREE_OBJECTID        ObjID = 1 // holds pointers to all of the tree roots
	EXTENT_TREE_OBJECTID      ObjID = 2 // stores information about which extents are in use, and reference counts
	CHUNK_TREE_OBJECTID       ObjID = 3 // chunk tree stores translations from logical -> physical block numbering
	DEV_TREE_OBJECTID         ObjID = 4 // stores info about which areas of a given device are in use; one per device
	FS_TREE_OBJECTID          ObjID = 5 // one per subvolume, storing files and directories
	ROOT_TREE_DIR_OBJECTID    ObjID = 6 // directory objectid inside the root tree
	CSUM_TREE_OBJECTID        ObjID = 7 // holds checksums of all the data extents
	QUOTA_TREE_OBJECTID       ObjID = 8
	UUID_TREE_OBJECTID        ObjID = 9  // for storing items that use the UUID_*_KEY
	FREE_SPACE_TREE_OBJECTID  ObjID = 10 // tracks free space in block groups.
	BLOCK_GROUP_TREE_OBJECTID ObjID = 11 // hold the block group items.

	// Objects in the DEV_TREE
	DEV_STATS_OBJECTID ObjID = 0 // device stats in the device tree

	// ???
	BALANCE_OBJECTID         ObjID = maxUint64pp - 4 // for storing balance parameters in the root tree
	ORPHAN_OBJECTID          ObjID = maxUint64pp - 5 // orphan objectid for tracking unlinked/truncated files
	TREE_LOG_OBJECTID        ObjID = maxUint64pp - 6 // does write ahead logging to speed up fsyncs
	TREE_LOG_FIXUP_OBJECTID  ObjID = maxUint64pp - 7
	TREE_RELOC_OBJECTID      ObjID = maxUint64pp - 8 // space balancing
	DATA_RELOC_TREE_OBJECTID ObjID = maxUint64pp - 9
	EXTENT_CSUM_OBJECTID     ObjID = maxUint64pp - 10 // extent checksums all have this objectid
	FREE_SPACE_OBJECTID      ObjID = maxUint64pp - 11 // For storing free space cache
	FREE_INO_OBJECTID        ObjID = maxUint64pp - 12 // stores the inode number for the free-ino cache

	MULTIPLE_OBJECTIDS ObjID = maxUint64pp - 255 // dummy objectid represents multiple objectids

	// All files have objectids in this range.
	FIRST_FREE_OBJECTID ObjID = 256
	LAST_FREE_OBJECTID  ObjID = maxUint64pp - 256

	// Objects in the CHUNK_TREE
	DEV_ITEMS_OBJECTID        ObjID = 1
	FIRST_CHUNK_TREE_OBJECTID ObjID = 256

	// ???
	EMPTY_SUBVOL_DIR_OBJECTID ObjID = 2
)

var (
	objidCommonNames = map[ObjID]string{
		BALANCE_OBJECTID:         "BALANCE",
		ORPHAN_OBJECTID:          "ORPHAN",
		TREE_LOG_OBJECTID:        "TREE_LOG",
		TREE_LOG_FIXUP_OBJECTID:  "TREE_LOG_FIXUP",
		TREE_RELOC_OBJECTID:      "TREE_RELOC",
		DATA_RELOC_TREE_OBJECTID: "DATA_RELOC_TREE",
		EXTENT_CSUM_OBJECTID:     "EXTENT_CSUM",
		FREE_SPACE_OBJECTID:      "FREE_SPACE",
		FREE_INO_OBJECTID:        "FREE_INO",
		MULTIPLE_OBJECTIDS:       "MULTIPLE",
	}
	objidDevTreeNames = map[ObjID]string{
		DEV_STATS_OBJECTID: "DEV_STATS",
	}
	objidChunkTreeNames = map[ObjID]string{
		DEV_ITEMS_OBJECTID:        "DEV_ITEMS",
		FIRST_CHUNK_TREE_OBJECTID: "FIRST_CHUNK_TREE",
	}
	objidRootTreeNames = map[ObjID]string{
		ROOT_TREE_OBJECTID:        "ROOT_TREE",
		EXTENT_TREE_OBJECTID:      "EXTENT_TREE",
		CHUNK_TREE_OBJECTID:       "CHUNK_TREE",
		DEV_TREE_OBJECTID:         "DEV_TREE",
		FS_TREE_OBJECTID:          "FS_TREE",
		ROOT_TREE_DIR_OBJECTID:    "ROOT_TREE_DIR",
		CSUM_TREE_OBJECTID:        "CSUM_TREE",
		QUOTA_TREE_OBJECTID:       "QUOTA_TREE",
		UUID_TREE_OBJECTID:        "UUID_TREE",
		FREE_SPACE_TREE_OBJECTID:  "FREE_SPACE_TREE",
		BLOCK_GROUP_TREE_OBJECTID: "BLOCK_GROUP_TREE",
	}
)

func (id ObjID) Format(tree ObjID) string {
	switch tree {
	case DEV_TREE_OBJECTID:
		if name, ok := objidDevTreeNames[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	case QUOTA_TREE_OBJECTID:
		if id == 0 {
			return "0"
		}
		//nolint:gomnd // The left 48 bits are the "qgroup level", and the right 16 bits are the subvolume ID.
		return fmt.Sprintf("%d/%d",
			uint64(id)>>48,
			uint64(id)&((1<<48)-1))
	case UUID_TREE_OBJECTID:
		return fmt.Sprintf("%#016x", uint64(id))
	case CHUNK_TREE_OBJECTID:
		if name, ok := objidCommonNames[id]; ok {
			return name
		}
		if name, ok := objidChunkTreeNames[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	default:
		if name, ok := objidCommonNames[id]; ok {
			return name
		}
		if name, ok := objidRootTreeNames[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	}
}

func (id ObjID) String() string {
	return id.Format(0)
}
