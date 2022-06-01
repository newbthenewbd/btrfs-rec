package btrfsitem

import "lukeshu.com/btrfs-tools/pkg/btrfs/internal"

type Type = internal.ItemType

const (
	CHUNK_ITEM_KEY           = internal.CHUNK_ITEM_KEY
	DEV_ITEM_KEY             = internal.DEV_ITEM_KEY
	DEV_EXTENT_KEY           = internal.DEV_EXTENT_KEY
	UNTYPED_KEY              = internal.UNTYPED_KEY
	QGROUP_RELATION_KEY      = internal.QGROUP_RELATION_KEY
	INODE_ITEM_KEY           = internal.INODE_ITEM_KEY
	INODE_REF_KEY            = internal.INODE_REF_KEY
	ORPHAN_ITEM_KEY          = internal.ORPHAN_ITEM_KEY
	PERSISTENT_ITEM_KEY      = internal.PERSISTENT_ITEM_KEY
	ROOT_ITEM_KEY            = internal.ROOT_ITEM_KEY
	UUID_SUBVOL_KEY          = internal.UUID_SUBVOL_KEY
	UUID_RECEIVED_SUBVOL_KEY = internal.UUID_RECEIVED_SUBVOL_KEY
)
