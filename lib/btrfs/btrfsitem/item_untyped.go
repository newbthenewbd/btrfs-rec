package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/internal"
)

type FreeSpaceHeader struct { // UNTYPED=0:FREE_SPACE_OBJECTID
	Location      internal.Key        `bin:"off=0x00, siz=0x11"`
	Generation    internal.Generation `bin:"off=0x11, siz=0x8"`
	NumEntries    int64               `bin:"off=0x19, siz=0x8"`
	NumBitmaps    int64               `bin:"off=0x21, siz=0x8"`
	binstruct.End `bin:"off=0x29"`
}
