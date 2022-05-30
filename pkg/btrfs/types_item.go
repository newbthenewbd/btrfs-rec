package btrfs

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type ItemType uint8

const (
	BTRFS_UNTYPED_KEY = ItemType(0)

	// inode items have the data typically returned from stat and store other
	// info about object characteristics.  There is one for every file and dir in
	// the FS
	BTRFS_INODE_ITEM_KEY   = ItemType(1)
	BTRFS_INODE_REF_KEY    = ItemType(12)
	BTRFS_INODE_EXTREF_KEY = ItemType(13)
	BTRFS_XATTR_ITEM_KEY   = ItemType(24)

	BTRFS_VERITY_DESC_ITEM_KEY   = ItemType(36) // new
	BTRFS_VERITY_MERKLE_ITEM_KEY = ItemType(37) // new

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
	BTRFS_CSUM_ITEM_KEY = ItemType(120) // new
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
	BTRFS_METADATA_ITEM_KEY = ItemType(169) // new

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
	BTRFS_FREE_SPACE_INFO_KEY = ItemType(198) // new

	// A free space extent tracks an extent of space that is free in a block group.
	// It is keyed on (start, FREE_SPACE_EXTENT, length).
	BTRFS_FREE_SPACE_EXTENT_KEY = ItemType(199) // new

	// When a block group becomes very fragmented, we convert it to use bitmaps
	// instead of extents. A free space bitmap is keyed on
	// (start, FREE_SPACE_BITMAP, length); the corresponding item is a bitmap with
	// (length / sectorsize) bits.
	BTRFS_FREE_SPACE_BITMAP_KEY = ItemType(200) // new

	BTRFS_DEV_EXTENT_KEY = ItemType(204)
	BTRFS_DEV_ITEM_KEY   = ItemType(216)
	BTRFS_CHUNK_ITEM_KEY = ItemType(228)

	// quota groups
	BTRFS_QGROUP_STATUS_KEY   = ItemType(240) // new
	BTRFS_QGROUP_INFO_KEY     = ItemType(242) // new
	BTRFS_QGROUP_LIMIT_KEY    = ItemType(244) // new
	BTRFS_QGROUP_RELATION_KEY = ItemType(246) // new

	// The key type for tree items that are stored persistently, but do not need to
	// exist for extended period of time. The items can exist in any tree.
	//
	// [subtype, BTRFS_TEMPORARY_ITEM_KEY, data]
	//
	// Existing items:
	//
	// - balance status item
	//   (BTRFS_BALANCE_OBJECTID, BTRFS_TEMPORARY_ITEM_KEY, 0)
	BTRFS_TEMPORARY_ITEM_KEY = ItemType(248) // new

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
	BTRFS_PERSISTENT_ITEM_KEY = ItemType(249) // new

	// Persistently stores the device replace state in the device tree.
	// The key is built like this: (0, BTRFS_DEV_REPLACE_KEY, 0).
	BTRFS_DEV_REPLACE_KEY = ItemType(250)

	// Stores items that allow to quickly map UUIDs to something else.
	// These items are part of the filesystem UUID tree.
	// The key is built like this:
	// (UUID_upper_64_bits, BTRFS_UUID_KEY*, UUID_lower_64_bits).
	BTRFS_UUID_KEY_SUBVOL          = ItemType(251) // for UUIDs assigned to subvols // new
	BTRFS_UUID_KEY_RECEIVED_SUBVOL = ItemType(252) // for UUIDs assigned to received subvols // new

	// string items are for debugging.  They just store a short string of
	// data in the FS
	BTRFS_STRING_ITEM_KEY = ItemType(253)
)

func (t ItemType) String() string {
	names := map[ItemType]string{
		BTRFS_UNTYPED_KEY:              "UNTYPED",
		BTRFS_INODE_ITEM_KEY:           "INODE_ITEM",
		BTRFS_INODE_REF_KEY:            "INODE_REF",
		BTRFS_INODE_EXTREF_KEY:         "INODE_EXTREF",
		BTRFS_XATTR_ITEM_KEY:           "XATTR_ITEM",
		BTRFS_VERITY_DESC_ITEM_KEY:     "VERITY_DESC_ITEM",
		BTRFS_VERITY_MERKLE_ITEM_KEY:   "VERITY_MERKLE_ITEM",
		BTRFS_ORPHAN_ITEM_KEY:          "ORPHAN_ITEM",
		BTRFS_DIR_LOG_ITEM_KEY:         "DIR_LOG_ITEM",
		BTRFS_DIR_LOG_INDEX_KEY:        "DIR_LOG_INDEX",
		BTRFS_DIR_ITEM_KEY:             "DIR_ITEM",
		BTRFS_DIR_INDEX_KEY:            "DIR_INDEX",
		BTRFS_EXTENT_DATA_KEY:          "EXTENT_DATA",
		BTRFS_CSUM_ITEM_KEY:            "CSUM_ITEM",
		BTRFS_EXTENT_CSUM_KEY:          "EXTENT_CSUM",
		BTRFS_ROOT_ITEM_KEY:            "ROOT_ITEM",
		BTRFS_ROOT_BACKREF_KEY:         "ROOT_BACKREF",
		BTRFS_ROOT_REF_KEY:             "ROOT_REF",
		BTRFS_EXTENT_ITEM_KEY:          "EXTENT_ITEM",
		BTRFS_METADATA_ITEM_KEY:        "METADATA_ITEM",
		BTRFS_TREE_BLOCK_REF_KEY:       "TREE_BLOCK_REF",
		BTRFS_EXTENT_DATA_REF_KEY:      "EXTENT_DATA_REF",
		BTRFS_EXTENT_REF_V0_KEY:        "EXTENT_REF_V0",
		BTRFS_SHARED_BLOCK_REF_KEY:     "SHARED_BLOCK_REF",
		BTRFS_SHARED_DATA_REF_KEY:      "SHARED_DATA_REF",
		BTRFS_BLOCK_GROUP_ITEM_KEY:     "BLOCK_GROUP_ITEM",
		BTRFS_FREE_SPACE_INFO_KEY:      "FREE_SPACE_INFO",
		BTRFS_FREE_SPACE_EXTENT_KEY:    "FREE_SPACE_EXTENT",
		BTRFS_FREE_SPACE_BITMAP_KEY:    "FREE_SPACE_BITMAP",
		BTRFS_DEV_EXTENT_KEY:           "DEV_EXTENT",
		BTRFS_DEV_ITEM_KEY:             "DEV_ITEM",
		BTRFS_CHUNK_ITEM_KEY:           "CHUNK_ITEM",
		BTRFS_QGROUP_STATUS_KEY:        "QGROUP_STATUS",
		BTRFS_QGROUP_INFO_KEY:          "QGROUP_INFO",
		BTRFS_QGROUP_LIMIT_KEY:         "QGROUP_LIMIT",
		BTRFS_QGROUP_RELATION_KEY:      "QGROUP_RELATION",
		BTRFS_TEMPORARY_ITEM_KEY:       "TEMPORARY_ITEM",
		BTRFS_PERSISTENT_ITEM_KEY:      "PERSISTENT_ITEM",
		BTRFS_DEV_REPLACE_KEY:          "DEV_REPLACE",
		BTRFS_UUID_KEY_SUBVOL:          "UUID_KEY_SUBVOL",
		BTRFS_UUID_KEY_RECEIVED_SUBVOL: "UUID_KEY_RECEIVED_SUBVOL",
		BTRFS_STRING_ITEM_KEY:          "STRING_ITEM",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("%d", t)
}

type DevItem struct {
	DeviceID ObjID `bin:"off=0,    siz=8"` // device ID

	NumBytes     uint64 `bin:"off=8,    siz=8"` // number of bytes
	NumBytesUsed uint64 `bin:"off=10,   siz=8"` // number of bytes used

	IOOptimalAlign uint32 `bin:"off=18,   siz=4"` // optimal I/O align
	IOOptimalWidth uint32 `bin:"off=1c,   siz=4"` // optimal I/O width
	IOMinSize      uint32 `bin:"off=20,   siz=4"` // minimal I/O size (sector size)

	Type        uint64     `bin:"off=24,   siz=8"` // type
	Generation  Generation `bin:"off=2c,   siz=8"` // generation
	StartOffset uint64     `bin:"off=34,   siz=8"` // start offset
	DevGroup    uint32     `bin:"off=3c,   siz=4"` // dev group
	SeekSpeed   uint8      `bin:"off=40,   siz=1"` // seek speed
	Bandwidth   uint8      `bin:"off=41,   siz=1"` // bandwidth

	DevUUID UUID `bin:"off=42,   siz=10"` // device UUID
	FSUUID  UUID `bin:"off=52,   siz=10"` // FS UUID

	binstruct.End `bin:"off=62"`
}

type InodeRefItem struct {
	Index         int64 `bin:"off=0, siz=8"`
	NameLen       int16 `bin:"off=8, siz=2"`
	binstruct.End `bin:"off=a"`
	Name          []byte `bin:"-"`
}

type InodeItem struct {
	Generation    int64    `bin:"off=0, siz=8"`
	TransID       int64    `bin:"off=8, siz=8"`
	Size          int64    `bin:"off=10, siz=8"`
	NumBytes      int64    `bin:"off=18, siz=8"`
	BlockGroup    int64    `bin:"off=20, siz=8"`
	NLink         int32    `bin:"off=28, siz=4"`
	UID           int32    `bin:"off=2C, siz=4"`
	GID           int32    `bin:"off=30, siz=4"`
	Mode          int32    `bin:"off=34, siz=4"`
	RDev          int64    `bin:"off=38, siz=8"`
	Flags         uint64   `bin:"off=40, siz=8"`
	Sequence      int64    `bin:"off=48, siz=8"`
	Reserved      [4]int64 `bin:"off=50, siz=20"`
	ATime         Time     `bin:"off=70, siz=c"`
	CTime         Time     `bin:"off=7c, siz=c"`
	MTime         Time     `bin:"off=88, siz=c"`
	OTime         Time     `bin:"off=94, siz=c"`
	binstruct.End `bin:"off=a0"`
}

type RootItem struct {
	Inode         InodeItem     `bin:"off=0, siz=a0"`
	Generation    int64         `bin:"off=a0, siz=8"`
	RootDirID     int64         `bin:"off=a8, siz=8"`
	ByteNr        LogicalAddr   `bin:"off=b0, siz=8"`
	ByteLimit     int64         `bin:"off=b8, siz=8"`
	BytesUsed     int64         `bin:"off=c0, siz=8"`
	LastSnapshot  int64         `bin:"off=c8, siz=8"`
	Flags         RootItemFlags `bin:"off=d0, siz=8"`
	Refs          int32         `bin:"off=d8, siz=4"`
	DropProgress  Key           `bin:"off=dc, siz=11"`
	DropLevel     uint8         `bin:"off=ed, siz=1"`
	Level         uint8         `bin:"off=ee, siz=1"`
	GenerationV2  int64         `bin:"off=ef, siz=8"`
	UUID          UUID          `bin:"off=F7, siz=10"`
	ParentUUID    UUID          `bin:"off=107, siz=10"`
	ReceivedUUID  UUID          `bin:"off=117, siz=10"`
	CTransID      int64         `bin:"off=127, siz=8"`
	OTransID      int64         `bin:"off=12f, siz=8"`
	STransID      int64         `bin:"off=137, siz=8"`
	RTransID      int64         `bin:"off=13f, siz=8"`
	CTime         Time          `bin:"off=147, siz=c"`
	OTime         Time          `bin:"off=153, siz=c"`
	STime         Time          `bin:"off=15F, siz=c"`
	RTime         Time          `bin:"off=16b, siz=c"`
	GlobalTreeID  ObjID         `bin:"off=177, siz=8"`
	Reserved      [7]int64      `bin:"off=17f, siz=38"`
	binstruct.End `bin:"off=1b7"`
}
