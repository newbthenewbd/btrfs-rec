package internal

import "fmt"

type ItemType uint8

const (
	CHUNK_ITEM_KEY           = ItemType(228)
	DEV_ITEM_KEY             = ItemType(216)
	DEV_EXTENT_KEY           = ItemType(204)
	UNTYPED_KEY              = ItemType(0)
	QGROUP_RELATION_KEY      = ItemType(246)
	INODE_ITEM_KEY           = ItemType(1)
	INODE_REF_KEY            = ItemType(12)
	ORPHAN_ITEM_KEY          = ItemType(48)
	PERSISTENT_ITEM_KEY      = ItemType(249)
	ROOT_ITEM_KEY            = ItemType(132)
	UUID_SUBVOL_KEY          = ItemType(251)
	UUID_RECEIVED_SUBVOL_KEY = ItemType(252)
)

func (t ItemType) String() string {
	names := map[ItemType]string{
		CHUNK_ITEM_KEY:           "CHUNK_ITEM",
		DEV_ITEM_KEY:             "DEV_ITEM",
		DEV_EXTENT_KEY:           "DEV_EXTENT",
		UNTYPED_KEY:              "UNTYPED",
		QGROUP_RELATION_KEY:      "QGROUP_RELATION",
		INODE_ITEM_KEY:           "INODE_ITEM",
		INODE_REF_KEY:            "INODE_REF",
		ORPHAN_ITEM_KEY:          "ORPHAN_ITEM",
		PERSISTENT_ITEM_KEY:      "PERSISTENT_ITEM",
		ROOT_ITEM_KEY:            "ROOT_ITEM",
		UUID_SUBVOL_KEY:          "UUID_SUBVOL",
		UUID_RECEIVED_SUBVOL_KEY: "UUID_RECEIVED_SUBVOL",
	}
	if name, ok := names[t]; ok {
		return name
	}
	return fmt.Sprintf("%d", t)
}
