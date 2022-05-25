package btrfs

import (
	"fmt"
)

type ItemType uint8

const (
	// inode items have the data typically returned from stat and store other
	// info about object characteristics.  There is one for every file and dir in
	// the FS
	BTRFS_INODE_ITEM_KEY   = ItemType(1)
	BTRFS_INODE_REF_KEY    = ItemType(12)
	BTRFS_INODE_EXTREF_KEY = ItemType(13)
	BTRFS_XATTR_ITEM_KEY   = ItemType(24)

	BTRFS_VERITY_DESC_ITEM_KEY   = ItemType(36)
	BTRFS_VERITY_MERKLE_ITEM_KEY = ItemType(37)

	BTRFS_ORPHAN_ITEM_KEY = ItemType(48)

	BTRFS_DIR_LOG_ITEM_KEY  = ItemType(60)
	BTRFS_DIR_LOG_INDEX_KEY = ItemType(72)
	// dir items are the name -> inode pointers in a directory.  There is one
	// for every name in a directory.
	BTRFS_DIR_ITEM_KEY  = ItemType(84)
	BTRFS_DIR_INDEX_KEY = ItemType(96)

	// extent data is for file data
	BTRFS_EXTENT_DATA_KEY = ItemType(108)

	// csum items have the checksums for data in the extents
	BTRFS_CSUM_ITEM_KEY = ItemType(120)
	// extent csums are stored in a separate tree and hold csums for
	// an entire extent on disk.
	BTRFS_EXTENT_CSUM_KEY = ItemType(128)

	// root items point to tree roots.  There are typically in the root
	// tree used by the super block to find all the other trees
	BTRFS_ROOT_ITEM_KEY = ItemType(132)

	// root backrefs tie subvols and snapshots to the directory entries that
	// reference them
	BTRFS_ROOT_BACKREF_KEY = ItemType(144)

	// root refs make a fast index for listing all of the snapshots and
	// subvolumes referenced by a given root.  They point directly to the
	// directory item in the root that references the subvol
	BTRFS_ROOT_REF_KEY = ItemType(156)

	// extent items are in the extent map tree.  These record which blocks
	// are used, and how many references there are to each block
	BTRFS_EXTENT_ITEM_KEY = ItemType(168)

	// The same as the BTRFS_EXTENT_ITEM_KEY, except it's metadata we already know
	// the length, so we save the level in key->offset instead of the length.
	BTRFS_METADATA_ITEM_KEY = ItemType(169)

	BTRFS_TREE_BLOCK_REF_KEY = ItemType(176)

	BTRFS_EXTENT_DATA_REF_KEY = ItemType(178)

	// old style extent backrefs
	BTRFS_EXTENT_REF_V0_KEY = ItemType(180)

	BTRFS_SHARED_BLOCK_REF_KEY = ItemType(182)

	BTRFS_SHARED_DATA_REF_KEY = ItemType(184)

	// block groups give us hints into the extent allocation trees.  Which
	// blocks are free etc etc
	BTRFS_BLOCK_GROUP_ITEM_KEY = ItemType(192)

	// Every block group is represented in the free space tree by a free space info
	// item, which stores some accounting information. It is keyed on
	// (block_group_start, FREE_SPACE_INFO, block_group_length).
	BTRFS_FREE_SPACE_INFO_KEY = ItemType(198)

	// A free space extent tracks an extent of space that is free in a block group.
	// It is keyed on (start, FREE_SPACE_EXTENT, length).
	BTRFS_FREE_SPACE_EXTENT_KEY = ItemType(199)

	// When a block group becomes very fragmented, we convert it to use bitmaps
	// instead of extents. A free space bitmap is keyed on
	// (start, FREE_SPACE_BITMAP, length); the corresponding item is a bitmap with
	// (length / sectorsize) bits.
	BTRFS_FREE_SPACE_BITMAP_KEY = ItemType(200)

	BTRFS_DEV_EXTENT_KEY = ItemType(204)
	BTRFS_DEV_ITEM_KEY   = ItemType(216)
	BTRFS_CHUNK_ITEM_KEY = ItemType(228)

	// quota groups
	BTRFS_QGROUP_STATUS_KEY   = ItemType(240)
	BTRFS_QGROUP_INFO_KEY     = ItemType(242)
	BTRFS_QGROUP_LIMIT_KEY    = ItemType(244)
	BTRFS_QGROUP_RELATION_KEY = ItemType(246)

	// The key type for tree items that are stored persistently, but do not need to
	// exist for extended period of time. The items can exist in any tree.
	//
	// [subtype, BTRFS_TEMPORARY_ITEM_KEY, data]
	//
	// Existing items:
	//
	// - balance status item
	//   (BTRFS_BALANCE_OBJECTID, BTRFS_TEMPORARY_ITEM_KEY, 0)
	BTRFS_TEMPORARY_ITEM_KEY = ItemType(248)

	// The key type for tree items that are stored persistently and usually exist
	// for a long period, eg. filesystem lifetime. The item kinds can be status
	// information, stats or preference values. The item can exist in any tree.
	//
	// [subtype, BTRFS_PERSISTENT_ITEM_KEY, data]
	//
	// Existing items:
	//
	// - device statistics, store IO stats in the device tree, one key for all
	//   stats
	//   (BTRFS_DEV_STATS_OBJECTID, BTRFS_DEV_STATS_KEY, 0)
	BTRFS_PERSISTENT_ITEM_KEY = ItemType(249)

	// Persistently stores the device replace state in the device tree.
	// The key is built like this: (0, BTRFS_DEV_REPLACE_KEY, 0).
	BTRFS_DEV_REPLACE_KEY = ItemType(250)

	// Stores items that allow to quickly map UUIDs to something else.
	// These items are part of the filesystem UUID tree.
	// The key is built like this:
	// (UUID_upper_64_bits, BTRFS_UUID_KEY*, UUID_lower_64_bits).
	BTRFS_UUID_KEY_SUBVOL          = ItemType(251) // for UUIDs assigned to subvols
	BTRFS_UUID_KEY_RECEIVED_SUBVOL = ItemType(252) // for UUIDs assigned to received subvols

	// string items are for debugging.  They just store a short string of
	// data in the FS
	BTRFS_STRING_ITEM_KEY = ItemType(253)
)

func (t ItemType) String() string {
	names := map[ItemType]string{
		BTRFS_INODE_ITEM_KEY:           "BTRFS_INODE_ITEM_KEY",
		BTRFS_INODE_REF_KEY:            "BTRFS_INODE_REF_KEY",
		BTRFS_INODE_EXTREF_KEY:         "BTRFS_INODE_EXTREF_KEY",
		BTRFS_XATTR_ITEM_KEY:           "BTRFS_XATTR_ITEM_KEY",
		BTRFS_VERITY_DESC_ITEM_KEY:     "BTRFS_VERITY_DESC_ITEM_KEY",
		BTRFS_VERITY_MERKLE_ITEM_KEY:   "BTRFS_VERITY_MERKLE_ITEM_KEY",
		BTRFS_ORPHAN_ITEM_KEY:          "BTRFS_ORPHAN_ITEM_KEY",
		BTRFS_DIR_LOG_ITEM_KEY:         "BTRFS_DIR_LOG_ITEM_KEY",
		BTRFS_DIR_LOG_INDEX_KEY:        "BTRFS_DIR_LOG_INDEX_KEY",
		BTRFS_DIR_ITEM_KEY:             "BTRFS_DIR_ITEM_KEY",
		BTRFS_DIR_INDEX_KEY:            "BTRFS_DIR_INDEX_KEY",
		BTRFS_EXTENT_DATA_KEY:          "BTRFS_EXTENT_DATA_KEY",
		BTRFS_CSUM_ITEM_KEY:            "BTRFS_CSUM_ITEM_KEY",
		BTRFS_EXTENT_CSUM_KEY:          "BTRFS_EXTENT_CSUM_KEY",
		BTRFS_ROOT_ITEM_KEY:            "BTRFS_ROOT_ITEM_KEY",
		BTRFS_ROOT_BACKREF_KEY:         "BTRFS_ROOT_BACKREF_KEY",
		BTRFS_ROOT_REF_KEY:             "BTRFS_ROOT_REF_KEY",
		BTRFS_EXTENT_ITEM_KEY:          "BTRFS_EXTENT_ITEM_KEY",
		BTRFS_METADATA_ITEM_KEY:        "BTRFS_METADATA_ITEM_KEY",
		BTRFS_TREE_BLOCK_REF_KEY:       "BTRFS_TREE_BLOCK_REF_KEY",
		BTRFS_EXTENT_DATA_REF_KEY:      "BTRFS_EXTENT_DATA_REF_KEY",
		BTRFS_EXTENT_REF_V0_KEY:        "BTRFS_EXTENT_REF_V0_KEY",
		BTRFS_SHARED_BLOCK_REF_KEY:     "BTRFS_SHARED_BLOCK_REF_KEY",
		BTRFS_SHARED_DATA_REF_KEY:      "BTRFS_SHARED_DATA_REF_KEY",
		BTRFS_BLOCK_GROUP_ITEM_KEY:     "BTRFS_BLOCK_GROUP_ITEM_KEY",
		BTRFS_FREE_SPACE_INFO_KEY:      "BTRFS_FREE_SPACE_INFO_KEY",
		BTRFS_FREE_SPACE_EXTENT_KEY:    "BTRFS_FREE_SPACE_EXTENT_KEY",
		BTRFS_FREE_SPACE_BITMAP_KEY:    "BTRFS_FREE_SPACE_BITMAP_KEY",
		BTRFS_DEV_EXTENT_KEY:           "BTRFS_DEV_EXTENT_KEY",
		BTRFS_DEV_ITEM_KEY:             "BTRFS_DEV_ITEM_KEY",
		BTRFS_CHUNK_ITEM_KEY:           "BTRFS_CHUNK_ITEM_KEY",
		BTRFS_QGROUP_STATUS_KEY:        "BTRFS_QGROUP_STATUS_KEY",
		BTRFS_QGROUP_INFO_KEY:          "BTRFS_QGROUP_INFO_KEY",
		BTRFS_QGROUP_LIMIT_KEY:         "BTRFS_QGROUP_LIMIT_KEY",
		BTRFS_QGROUP_RELATION_KEY:      "BTRFS_QGROUP_RELATION_KEY",
		BTRFS_TEMPORARY_ITEM_KEY:       "BTRFS_TEMPORARY_ITEM_KEY",
		BTRFS_PERSISTENT_ITEM_KEY:      "BTRFS_PERSISTENT_ITEM_KEY",
		BTRFS_DEV_REPLACE_KEY:          "BTRFS_DEV_REPLACE_KEY",
		BTRFS_UUID_KEY_SUBVOL:          "BTRFS_UUID_KEY_SUBVOL",
		BTRFS_UUID_KEY_RECEIVED_SUBVOL: "BTRFS_UUID_KEY_RECEIVED_SUBVOL",
		BTRFS_STRING_ITEM_KEY:          "BTRFS_STRING_ITEM_KEY",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("%d", t)
}
