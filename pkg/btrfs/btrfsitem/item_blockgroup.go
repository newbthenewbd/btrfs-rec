package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
	"lukeshu.com/btrfs-tools/pkg/util"
)

// key.objectid = logical_addr
// key.offset = size of chunk
type BlockGroup struct { // BLOCK_GROUP_ITEM=192
	Used          int64           `bin:"off=0, siz=8"`
	ChunkObjectID internal.ObjID  `bin:"off=8, siz=8"`
	Flags         BlockGroupFlags `bin:"off=16, siz=8"`
	binstruct.End `bin:"off=24"`
}

type BlockGroupFlags uint64

const (
	BLOCK_GROUP_DATA = BlockGroupFlags(1 << iota)
	BLOCK_GROUP_SYSTEM
	BLOCK_GROUP_METADATA
	BLOCK_GROUP_RAID0
	BLOCK_GROUP_RAID1
	BLOCK_GROUP_DUP
	BLOCK_GROUP_RAID10
	BLOCK_GROUP_RAID5
	BLOCK_GROUP_RAID6
	BLOCK_GROUP_RAID1C3
	BLOCK_GROUP_RAID1C4

	BLOCK_GROUP_RAID_MASK = (BLOCK_GROUP_RAID1 | BLOCK_GROUP_DUP | BLOCK_GROUP_RAID10 | BLOCK_GROUP_RAID5 | BLOCK_GROUP_RAID6 | BLOCK_GROUP_RAID1C3 | BLOCK_GROUP_RAID1C4)
)

var blockGroupFlagNames = []string{
	"DATA",
	"SYSTEM",
	"METADATA",

	"RAID0",
	"RAID1",
	"DUP",
	"RAID10",
	"RAID5",
	"RAID6",
	"RAID1C3",
	"RAID1C4",
}

func (f BlockGroupFlags) Has(req BlockGroupFlags) bool { return f&req == req }
func (f BlockGroupFlags) String() string {
	ret := util.BitfieldString(f, blockGroupFlagNames, util.HexNone)
	if f&BLOCK_GROUP_RAID_MASK == 0 {
		if ret == "" {
			ret = "single"
		} else {
			ret += "|single"
		}
	}
	return ret
}
