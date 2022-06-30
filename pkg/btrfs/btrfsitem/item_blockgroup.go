package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

// key.objectid = logical_addr
// key.offset = size of chunk
type BlockGroup struct { // BLOCK_GROUP_ITEM=192
	Used          int64                    `bin:"off=0, siz=8"`
	ChunkObjectID internal.ObjID           `bin:"off=8, siz=8"` // always BTRFS_FIRST_CHUNK_TREE_OBJECTID
	Flags         btrfsvol.BlockGroupFlags `bin:"off=16, siz=8"`
	binstruct.End `bin:"off=24"`
}
