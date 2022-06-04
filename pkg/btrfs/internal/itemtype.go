package internal

import "fmt"

type ItemType uint8

const (
	CHUNK_ITEM_KEY           = ItemType(228)
	DEV_ITEM_KEY             = ItemType(216)
	DEV_EXTENT_KEY           = ItemType(204)
	DIR_ITEM_KEY             = ItemType(84)
	DIR_INDEX_KEY            = ItemType(96)
	XATTR_ITEM_KEY           = ItemType(24)
	UNTYPED_KEY              = ItemType(0)
	TREE_BLOCK_REF_KEY       = ItemType(176)
	SHARED_BLOCK_REF_KEY     = ItemType(182)
	QGROUP_RELATION_KEY      = ItemType(246)
	EXTENT_ITEM_KEY          = ItemType(168)
	EXTENT_DATA_REF_KEY      = ItemType(178)
	INODE_ITEM_KEY           = ItemType(1)
	INODE_REF_KEY            = ItemType(12)
	METADATA_ITEM_KEY        = ItemType(169)
	ORPHAN_ITEM_KEY          = ItemType(48)
	PERSISTENT_ITEM_KEY      = ItemType(249)
	ROOT_ITEM_KEY            = ItemType(132)
	SHARED_DATA_REF_KEY      = ItemType(184)
	UUID_SUBVOL_KEY          = ItemType(251)
	UUID_RECEIVED_SUBVOL_KEY = ItemType(252)
)

func (t ItemType) String() string {
	names := map[ItemType]string{
		CHUNK_ITEM_KEY:           "CHUNK_ITEM",
		DEV_ITEM_KEY:             "DEV_ITEM",
		DEV_EXTENT_KEY:           "DEV_EXTENT",
		DIR_ITEM_KEY:             "DIR_ITEM",
		DIR_INDEX_KEY:            "DIR_INDEX",
		XATTR_ITEM_KEY:           "XATTR_ITEM",
		UNTYPED_KEY:              "UNTYPED",
		TREE_BLOCK_REF_KEY:       "TREE_BLOCK_REF",
		SHARED_BLOCK_REF_KEY:     "SHARED_BLOCK_REF",
		QGROUP_RELATION_KEY:      "QGROUP_RELATION",
		EXTENT_ITEM_KEY:          "EXTENT_ITEM",
		EXTENT_DATA_REF_KEY:      "EXTENT_DATA_REF",
		INODE_ITEM_KEY:           "INODE_ITEM",
		INODE_REF_KEY:            "INODE_REF",
		METADATA_ITEM_KEY:        "METADATA_ITEM",
		ORPHAN_ITEM_KEY:          "ORPHAN_ITEM",
		PERSISTENT_ITEM_KEY:      "PERSISTENT_ITEM",
		ROOT_ITEM_KEY:            "ROOT_ITEM",
		SHARED_DATA_REF_KEY:      "SHARED_DATA_REF",
		UUID_SUBVOL_KEY:          "UUID_SUBVOL",
		UUID_RECEIVED_SUBVOL_KEY: "UUID_RECEIVED_SUBVOL",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("%d", t)
}
