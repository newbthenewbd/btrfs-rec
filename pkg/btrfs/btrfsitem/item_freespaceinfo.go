package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type FreeSpaceInfo struct { // FREE_SPACE_INFO=198
	ExtentCount   int32  `bin:"off=0, siz=4"`
	Flags         uint32 `bin:"off=4, siz=4"`
	binstruct.End `bin:"off=8"`
}
