package btrfsitem

import (
	"reflect"

	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

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

var keytype2gotype = map[Type]reflect.Type{
	CHUNK_ITEM_KEY:           reflect.TypeOf(Chunk{}),
	DEV_ITEM_KEY:             reflect.TypeOf(Dev{}),
	DEV_EXTENT_KEY:           reflect.TypeOf(DevExtent{}),
	UNTYPED_KEY:              reflect.TypeOf(Empty{}),
	QGROUP_RELATION_KEY:      reflect.TypeOf(Empty{}),
	INODE_ITEM_KEY:           reflect.TypeOf(Inode{}),
	INODE_REF_KEY:            reflect.TypeOf(InodeRef{}),
	ORPHAN_ITEM_KEY:          reflect.TypeOf(Orphan{}),
	PERSISTENT_ITEM_KEY:      reflect.TypeOf(DevStats{}),
	ROOT_ITEM_KEY:            reflect.TypeOf(Root{}),
	UUID_SUBVOL_KEY:          reflect.TypeOf(UUIDMap{}),
	UUID_RECEIVED_SUBVOL_KEY: reflect.TypeOf(UUIDMap{}),
}

func (Chunk) isItem()     {}
func (Dev) isItem()       {}
func (DevExtent) isItem() {}
func (DevStats) isItem()  {}
func (Empty) isItem()     {}
func (Inode) isItem()     {}
func (InodeRef) isItem()  {}
func (Orphan) isItem()    {}
func (Root) isItem()      {}
func (UUIDMap) isItem()   {}
