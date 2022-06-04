package btrfsitem

import (
	"reflect"

	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

const (
	CHUNK_ITEM_KEY           = internal.CHUNK_ITEM_KEY
	DEV_ITEM_KEY             = internal.DEV_ITEM_KEY
	DEV_EXTENT_KEY           = internal.DEV_EXTENT_KEY
	DIR_ITEM_KEY             = internal.DIR_ITEM_KEY
	DIR_INDEX_KEY            = internal.DIR_INDEX_KEY
	XATTR_ITEM_KEY           = internal.XATTR_ITEM_KEY
	UNTYPED_KEY              = internal.UNTYPED_KEY
	TREE_BLOCK_REF_KEY       = internal.TREE_BLOCK_REF_KEY
	SHARED_BLOCK_REF_KEY     = internal.SHARED_BLOCK_REF_KEY
	QGROUP_RELATION_KEY      = internal.QGROUP_RELATION_KEY
	EXTENT_ITEM_KEY          = internal.EXTENT_ITEM_KEY
	EXTENT_DATA_REF_KEY      = internal.EXTENT_DATA_REF_KEY
	INODE_ITEM_KEY           = internal.INODE_ITEM_KEY
	INODE_REF_KEY            = internal.INODE_REF_KEY
	METADATA_ITEM_KEY        = internal.METADATA_ITEM_KEY
	ORPHAN_ITEM_KEY          = internal.ORPHAN_ITEM_KEY
	PERSISTENT_ITEM_KEY      = internal.PERSISTENT_ITEM_KEY
	ROOT_ITEM_KEY            = internal.ROOT_ITEM_KEY
	SHARED_DATA_REF_KEY      = internal.SHARED_DATA_REF_KEY
	UUID_SUBVOL_KEY          = internal.UUID_SUBVOL_KEY
	UUID_RECEIVED_SUBVOL_KEY = internal.UUID_RECEIVED_SUBVOL_KEY
)

var keytype2gotype = map[Type]reflect.Type{
	CHUNK_ITEM_KEY:           reflect.TypeOf(Chunk{}),
	DEV_ITEM_KEY:             reflect.TypeOf(Dev{}),
	DEV_EXTENT_KEY:           reflect.TypeOf(DevExtent{}),
	DIR_ITEM_KEY:             reflect.TypeOf(DirList{}),
	DIR_INDEX_KEY:            reflect.TypeOf(DirList{}),
	XATTR_ITEM_KEY:           reflect.TypeOf(DirList{}),
	UNTYPED_KEY:              reflect.TypeOf(Empty{}),
	TREE_BLOCK_REF_KEY:       reflect.TypeOf(Empty{}),
	SHARED_BLOCK_REF_KEY:     reflect.TypeOf(Empty{}),
	QGROUP_RELATION_KEY:      reflect.TypeOf(Empty{}),
	EXTENT_ITEM_KEY:          reflect.TypeOf(Extent{}),
	EXTENT_DATA_REF_KEY:      reflect.TypeOf(ExtentDataRef{}),
	INODE_ITEM_KEY:           reflect.TypeOf(Inode{}),
	INODE_REF_KEY:            reflect.TypeOf(InodeRefList{}),
	METADATA_ITEM_KEY:        reflect.TypeOf(Metadata{}),
	ORPHAN_ITEM_KEY:          reflect.TypeOf(Orphan{}),
	PERSISTENT_ITEM_KEY:      reflect.TypeOf(DevStats{}),
	ROOT_ITEM_KEY:            reflect.TypeOf(Root{}),
	SHARED_DATA_REF_KEY:      reflect.TypeOf(SharedDataRef{}),
	UUID_SUBVOL_KEY:          reflect.TypeOf(UUIDMap{}),
	UUID_RECEIVED_SUBVOL_KEY: reflect.TypeOf(UUIDMap{}),
}

func (Chunk) isItem()         {}
func (Dev) isItem()           {}
func (DevExtent) isItem()     {}
func (DevStats) isItem()      {}
func (DirList) isItem()       {}
func (Empty) isItem()         {}
func (Extent) isItem()        {}
func (ExtentDataRef) isItem() {}
func (Inode) isItem()         {}
func (InodeRefList) isItem()  {}
func (Metadata) isItem()      {}
func (Orphan) isItem()        {}
func (Root) isItem()          {}
func (SharedDataRef) isItem() {}
func (UUIDMap) isItem()       {}
