package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfstyp"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type Root struct { // ROOT_ITEM=132
	Inode         Inode                `bin:"off=0x0, siz=0xa0"`
	Generation    int64                `bin:"off=0xa0, siz=0x8"`
	RootDirID     int64                `bin:"off=0xa8, siz=0x8"`
	ByteNr        btrfstyp.LogicalAddr `bin:"off=0xb0, siz=0x8"`
	ByteLimit     int64                `bin:"off=0xb8, siz=0x8"`
	BytesUsed     int64                `bin:"off=0xc0, siz=0x8"`
	LastSnapshot  int64                `bin:"off=0xc8, siz=0x8"`
	Flags         RootFlags            `bin:"off=0xd0, siz=0x8"`
	Refs          int32                `bin:"off=0xd8, siz=0x4"`
	DropProgress  btrfstyp.Key         `bin:"off=0xdc, siz=0x11"`
	DropLevel     uint8                `bin:"off=0xed, siz=0x1"`
	Level         uint8                `bin:"off=0xee, siz=0x1"`
	GenerationV2  int64                `bin:"off=0xef, siz=0x8"`
	UUID          btrfstyp.UUID        `bin:"off=0xF7, siz=0x10"`
	ParentUUID    btrfstyp.UUID        `bin:"off=0x107, siz=0x10"`
	ReceivedUUID  btrfstyp.UUID        `bin:"off=0x117, siz=0x10"`
	CTransID      int64                `bin:"off=0x127, siz=0x8"`
	OTransID      int64                `bin:"off=0x12f, siz=0x8"`
	STransID      int64                `bin:"off=0x137, siz=0x8"`
	RTransID      int64                `bin:"off=0x13f, siz=0x8"`
	CTime         btrfstyp.Time        `bin:"off=0x147, siz=0xc"`
	OTime         btrfstyp.Time        `bin:"off=0x153, siz=0xc"`
	STime         btrfstyp.Time        `bin:"off=0x15F, siz=0xc"`
	RTime         btrfstyp.Time        `bin:"off=0x16b, siz=0xc"`
	GlobalTreeID  btrfstyp.ObjID       `bin:"off=0x177, siz=0x8"`
	Reserved      [7]int64             `bin:"off=0x17f, siz=0x38"`
	binstruct.End `bin:"off=0x1b7"`
}

type RootFlags uint64

const (
	BTRFS_ROOT_SUBVOL_RDONLY = RootFlags(1 << iota)
)

var rootItemFlagNames = []string{
	"SUBVOL_RDONLY",
}

func (f RootFlags) Has(req RootFlags) bool { return f&req == req }
func (f RootFlags) String() string         { return util.BitfieldString(f, rootItemFlagNames) }
