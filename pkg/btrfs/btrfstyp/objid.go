package btrfstyp

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type ObjID uint64

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
	BTRFS_UUID_TREE_OBJECTID        = ObjID(9)  // for storing items that use the BTRFS_UUID_*_KEY
	BTRFS_FREE_SPACE_TREE_OBJECTID  = ObjID(10) // tracks free space in block groups.
	BTRFS_BLOCK_GROUP_TREE_OBJECTID = ObjID(11) // hold the block group items.

	// Objects in the DEV_TREE
	BTRFS_DEV_STATS_OBJECTID = ObjID(0) // device stats in the device tree

	// ???
	BTRFS_BALANCE_OBJECTID         = ObjID(util.MaxUint64pp - 4) // for storing balance parameters in the root tree
	BTRFS_ORPHAN_OBJECTID          = ObjID(util.MaxUint64pp - 5) // orphan objectid for tracking unlinked/truncated files
	BTRFS_TREE_LOG_OBJECTID        = ObjID(util.MaxUint64pp - 6) // does write ahead logging to speed up fsyncs
	BTRFS_TREE_LOG_FIXUP_OBJECTID  = ObjID(util.MaxUint64pp - 7)
	BTRFS_TREE_RELOC_OBJECTID      = ObjID(util.MaxUint64pp - 8) // space balancing
	BTRFS_DATA_RELOC_TREE_OBJECTID = ObjID(util.MaxUint64pp - 9)
	BTRFS_EXTENT_CSUM_OBJECTID     = ObjID(util.MaxUint64pp - 10) // extent checksums all have this objectid
	BTRFS_FREE_SPACE_OBJECTID      = ObjID(util.MaxUint64pp - 11) // For storing free space cache
	BTRFS_FREE_INO_OBJECTID        = ObjID(util.MaxUint64pp - 12) // stores the inode number for the free-ino cache

	BTRFS_MULTIPLE_OBJECTIDS = ObjID(util.MaxUint64pp - 255) // dummy objectid represents multiple objectids

	// All files have objectids in this range.
	BTRFS_FIRST_FREE_OBJECTID = ObjID(256)
	BTRFS_LAST_FREE_OBJECTID  = ObjID(util.MaxUint64pp - 256)

	BTRFS_FIRST_CHUNK_TREE_OBJECTID = ObjID(256)

	// Objects in the CHUNK_TREE
	BTRFS_DEV_ITEMS_OBJECTID = ObjID(1)

	// ???
	BTRFS_EMPTY_SUBVOL_DIR_OBJECTID = ObjID(2)
)

func (id ObjID) Format(typ internal.ItemType) string {
	switch typ {
	case internal.PERSISTENT_ITEM_KEY:
		names := map[ObjID]string{
			BTRFS_DEV_STATS_OBJECTID: "DEV_STATS",
		}
		if name, ok := names[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	case internal.DEV_EXTENT_KEY:
		return fmt.Sprintf("%d", int64(id))
	case internal.QGROUP_RELATION_KEY:
		return fmt.Sprintf("%d/%d",
			uint64(id)>>48,
			uint64(id)&((1<<48)-1))
	case internal.UUID_SUBVOL_KEY, internal.UUID_RECEIVED_SUBVOL_KEY:
		return fmt.Sprintf("0x%016x", uint64(id))
	case internal.DEV_ITEM_KEY:
		names := map[ObjID]string{
			BTRFS_BALANCE_OBJECTID:         "BALANCE",
			BTRFS_ORPHAN_OBJECTID:          "ORPHAN",
			BTRFS_TREE_LOG_OBJECTID:        "TREE_LOG",
			BTRFS_TREE_LOG_FIXUP_OBJECTID:  "TREE_LOG_FIXUP",
			BTRFS_TREE_RELOC_OBJECTID:      "TREE_RELOC",
			BTRFS_DATA_RELOC_TREE_OBJECTID: "DATA_RELOC_TREE",
			BTRFS_EXTENT_CSUM_OBJECTID:     "EXTENT_CSUM",
			BTRFS_FREE_SPACE_OBJECTID:      "FREE_SPACE",
			BTRFS_FREE_INO_OBJECTID:        "FREE_INO",
			BTRFS_MULTIPLE_OBJECTIDS:       "MULTIPLE",

			BTRFS_DEV_ITEMS_OBJECTID: "DEV_ITEMS",
		}
		if name, ok := names[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	case internal.CHUNK_ITEM_KEY:
		names := map[ObjID]string{
			BTRFS_BALANCE_OBJECTID:         "BALANCE",
			BTRFS_ORPHAN_OBJECTID:          "ORPHAN",
			BTRFS_TREE_LOG_OBJECTID:        "TREE_LOG",
			BTRFS_TREE_LOG_FIXUP_OBJECTID:  "TREE_LOG_FIXUP",
			BTRFS_TREE_RELOC_OBJECTID:      "TREE_RELOC",
			BTRFS_DATA_RELOC_TREE_OBJECTID: "DATA_RELOC_TREE",
			BTRFS_EXTENT_CSUM_OBJECTID:     "EXTENT_CSUM",
			BTRFS_FREE_SPACE_OBJECTID:      "FREE_SPACE",
			BTRFS_FREE_INO_OBJECTID:        "FREE_INO",
			BTRFS_MULTIPLE_OBJECTIDS:       "MULTIPLE",

			BTRFS_FIRST_CHUNK_TREE_OBJECTID: "FIRST_CHUNK_TREE",
		}
		if name, ok := names[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	default:
		names := map[ObjID]string{
			BTRFS_BALANCE_OBJECTID:         "BALANCE",
			BTRFS_ORPHAN_OBJECTID:          "ORPHAN",
			BTRFS_TREE_LOG_OBJECTID:        "TREE_LOG",
			BTRFS_TREE_LOG_FIXUP_OBJECTID:  "TREE_LOG_FIXUP",
			BTRFS_TREE_RELOC_OBJECTID:      "TREE_RELOC",
			BTRFS_DATA_RELOC_TREE_OBJECTID: "DATA_RELOC_TREE",
			BTRFS_EXTENT_CSUM_OBJECTID:     "EXTENT_CSUM",
			BTRFS_FREE_SPACE_OBJECTID:      "FREE_SPACE",
			BTRFS_FREE_INO_OBJECTID:        "FREE_INO",
			BTRFS_MULTIPLE_OBJECTIDS:       "MULTIPLE",

			BTRFS_ROOT_TREE_OBJECTID:        "ROOT_TREE",
			BTRFS_EXTENT_TREE_OBJECTID:      "EXTENT_TREE",
			BTRFS_CHUNK_TREE_OBJECTID:       "CHUNK_TREE",
			BTRFS_DEV_TREE_OBJECTID:         "DEV_TREE",
			BTRFS_FS_TREE_OBJECTID:          "FS_TREE",
			BTRFS_ROOT_TREE_DIR_OBJECTID:    "ROOT_TREE_DIR",
			BTRFS_CSUM_TREE_OBJECTID:        "CSUM_TREE",
			BTRFS_QUOTA_TREE_OBJECTID:       "QUOTA_TREE",
			BTRFS_UUID_TREE_OBJECTID:        "UUID_TREE",
			BTRFS_FREE_SPACE_TREE_OBJECTID:  "FREE_SPACE_TREE",
			BTRFS_BLOCK_GROUP_TREE_OBJECTID: "BLOCK_GROUP_TREE",
		}
		if name, ok := names[id]; ok {
			return name
		}
		return fmt.Sprintf("%d", int64(id))
	}
}

func (id ObjID) String() string {
	return id.Format(internal.UNTYPED_KEY)
}
