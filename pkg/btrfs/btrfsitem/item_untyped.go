package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type FreeSpaceHeader struct { // UNTYPED=0:FREE_SPACE_OBJECTID
	Location      internal.Key `bin:"off=0x00, siz=0x11"`
	Generation    int64        `bin:"off=0x11, siz=0x8"`
	NumEntries    int64        `bin:"off=0x19, siz=0x8"`
	NumBitmaps    int64        `bin:"off=0x21, siz=0x8"`
	binstruct.End `bin:"off=0x29"`
}
