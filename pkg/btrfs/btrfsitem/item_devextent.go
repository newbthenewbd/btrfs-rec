package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfstyp"
)

type DevExtent struct { // DEV_EXTENT=204
	ChunkTree     int64          `bin:"off=0, siz=8"`
	ChunkObjectID btrfstyp.ObjID `bin:"off=8, siz=8"`
	ChunkOffset   int64          `bin:"off=16, siz=8"`
	Length        int64          `bin:"off=24, siz=8"`
	ChunkTreeUUID btrfstyp.UUID  `bin:"off=32, siz=16"`
	binstruct.End `bin:"off=48"`
}
