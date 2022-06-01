package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type Inode struct { // INODE_ITEM=1
	Generation    int64         `bin:"off=0x0, siz=0x8"`
	TransID       int64         `bin:"off=0x8, siz=0x8"`
	Size          int64         `bin:"off=0x10, siz=0x8"`
	NumBytes      int64         `bin:"off=0x18, siz=0x8"`
	BlockGroup    int64         `bin:"off=0x20, siz=0x8"`
	NLink         int32         `bin:"off=0x28, siz=0x4"`
	UID           int32         `bin:"off=0x2C, siz=0x4"`
	GID           int32         `bin:"off=0x30, siz=0x4"`
	Mode          int32         `bin:"off=0x34, siz=0x4"`
	RDev          int64         `bin:"off=0x38, siz=0x8"`
	Flags         uint64        `bin:"off=0x40, siz=0x8"`
	Sequence      int64         `bin:"off=0x48, siz=0x8"`
	Reserved      [4]int64      `bin:"off=0x50, siz=0x20"`
	ATime         internal.Time `bin:"off=0x70, siz=0xc"`
	CTime         internal.Time `bin:"off=0x7c, siz=0xc"`
	MTime         internal.Time `bin:"off=0x88, siz=0xc"`
	OTime         internal.Time `bin:"off=0x94, siz=0xc"`
	binstruct.End `bin:"off=0xa0"`
}
