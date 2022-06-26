package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type DevExtent struct { // DEV_EXTENT=204
	ChunkTree     int64                `bin:"off=0, siz=8"`
	ChunkObjectID internal.ObjID       `bin:"off=8, siz=8"`
	ChunkOffset   internal.LogicalAddr `bin:"off=16, siz=8"`
	Length        uint64               `bin:"off=24, siz=8"`
	ChunkTreeUUID internal.UUID        `bin:"off=32, siz=16"`
	binstruct.End `bin:"off=48"`
}
