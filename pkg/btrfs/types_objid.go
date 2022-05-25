package btrfs

import (
	"fmt"
)

type ObjID uint64

const maxUint64pp = 0x1_0000_0000

const (
	// The IDs of the various trees
	BTRFS_ROOT_TREE_OBJECTID        = ObjID(1) // holds pointers to all of the tree roots
	BTRFS_EXTENT_TREE_OBJECTID      = ObjID(2) // stores information about which extents are in use, and reference counts
	BTRFS_CHUNK_TREE_OBJECTID       = ObjID(3) // chunk tree stores translations from logical -> physical block numbering
	BTRFS_DEV_TREE_OBJECTID         = ObjID(4) // stores info about which areas of a given device are in use; one per device
	BTRFS_FS_TREE_OBJECTID          = ObjID(5) // one per subvolume, storing files and directories
	BTRFS_ROOT_TREE_DIR_OBJECTID    = ObjID(6) // directory objectid inside the root tree
	BTRFS_CSUM_TREE_OBJECTID        = ObjID(7) // holds checksums of all the data extents
	BTRFS_QUOTA_TREE_OBJECTID       = ObjID(8)
	BTRFS_UUID_TREE_OBJECTID        = ObjID(9)  // for storing items that use the BTRFS_UUID_KEY*
	BTRFS_FREE_SPACE_TREE_OBJECTID  = ObjID(10) // tracks free space in block groups.
	BTRFS_BLOCK_GROUP_TREE_OBJECTID = ObjID(11) // hold the block group items.

	// Objects in the DEV_TREE
	BTRFS_DEV_STATS_OBJECTID = ObjID(0) // device stats in the device tree

	// ???
	BTRFS_BALANCE_OBJECTID         = ObjID(maxUint64pp - 4) // for storing balance parameters in the root tree
	BTRFS_ORPHAN_OBJECTID          = ObjID(maxUint64pp - 5) // orphan objectid for tracking unlinked/truncated files
	BTRFS_TREE_LOG_OBJECTID        = ObjID(maxUint64pp - 6) // does write ahead logging to speed up fsyncs
	BTRFS_TREE_LOG_FIXUP_OBJECTID  = ObjID(maxUint64pp - 7)
	BTRFS_TREE_RELOC_OBJECTID      = ObjID(maxUint64pp - 8) // space balancing
	BTRFS_DATA_RELOC_TREE_OBJECTID = ObjID(maxUint64pp - 9)
	BTRFS_EXTENT_CSUM_OBJECTID     = ObjID(maxUint64pp - 10) // extent checksums all have this objectid
	BTRFS_FREE_SPACE_OBJECTID      = ObjID(maxUint64pp - 11) // For storing free space cache
	BTRFS_FREE_INO_OBJECTID        = ObjID(maxUint64pp - 12) // stores the inode number for the free-ino cache

	BTRFS_MULTIPLE_OBJECTIDS = ObjID(maxUint64pp - 255) // dummy objectid represents multiple objectids

	// All files have objectids in this range.
	BTRFS_FIRST_FREE_OBJECTID = ObjID(256)
	BTRFS_LAST_FREE_OBJECTID  = ObjID(maxUint64pp - 256)

	BTRFS_FIRST_CHUNK_TREE_OBJECTID = ObjID(256)

	// Objects in the CHUNK_TREE
	BTRFS_DEV_ITEMS_OBJECTID = ObjID(1)

	// ???
	BTRFS_EMPTY_SUBVOL_DIR_OBJECTID = ObjID(2)
)

func (id ObjID) String() string {
	if id > BTRFS_LAST_FREE_OBJECTID {
		names := map[ObjID]string{
			BTRFS_BALANCE_OBJECTID:         "BTRFS_BALANCE_OBJECTID",
			BTRFS_ORPHAN_OBJECTID:          "BTRFS_ORPHAN_OBJECTID",
			BTRFS_TREE_LOG_OBJECTID:        "BTRFS_TREE_LOG_OBJECTID",
			BTRFS_TREE_LOG_FIXUP_OBJECTID:  "BTRFS_TREE_LOG_FIXUP_OBJECTID",
			BTRFS_TREE_RELOC_OBJECTID:      "BTRFS_TREE_RELOC_OBJECTID",
			BTRFS_DATA_RELOC_TREE_OBJECTID: "BTRFS_DATA_RELOC_TREE_OBJECTID",
			BTRFS_EXTENT_CSUM_OBJECTID:     "BTRFS_EXTENT_CSUM_OBJECTID",
			BTRFS_FREE_SPACE_OBJECTID:      "BTRFS_FREE_SPACE_OBJECTID",
			BTRFS_FREE_INO_OBJECTID:        "BTRFS_FREE_INO_OBJECTID",
			BTRFS_MULTIPLE_OBJECTIDS:       "BTRFS_MULTIPLE_OBJECTIDS",
		}
		if name, ok := names[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	}
	return fmt.Sprintf("%d", id)
}

type TreeObjID ObjID

func (id TreeObjID) String() string {
	names := map[ObjID]string{
		BTRFS_ROOT_TREE_OBJECTID:        "BTRFS_ROOT_TREE_OBJECTID",
		BTRFS_EXTENT_TREE_OBJECTID:      "BTRFS_EXTENT_TREE_OBJECTID",
		BTRFS_CHUNK_TREE_OBJECTID:       "BTRFS_CHUNK_TREE_OBJECTID",
		BTRFS_DEV_TREE_OBJECTID:         "BTRFS_DEV_TREE_OBJECTID",
		BTRFS_FS_TREE_OBJECTID:          "BTRFS_FS_TREE_OBJECTID",
		BTRFS_ROOT_TREE_DIR_OBJECTID:    "BTRFS_ROOT_TREE_DIR_OBJECTID",
		BTRFS_CSUM_TREE_OBJECTID:        "BTRFS_CSUM_TREE_OBJECTID",
		BTRFS_QUOTA_TREE_OBJECTID:       "BTRFS_QUOTA_TREE_OBJECTID",
		BTRFS_UUID_TREE_OBJECTID:        "BTRFS_UUID_TREE_OBJECTID",
		BTRFS_FREE_SPACE_TREE_OBJECTID:  "BTRFS_FREE_SPACE_TREE_OBJECTID",
		BTRFS_BLOCK_GROUP_TREE_OBJECTID: "BTRFS_BLOCK_GROUP_TREE_OBJECTID",
	}
	if name, ok := names[ObjID(id)]; ok {
		return name
	}
	return ObjID(id).String()
}
