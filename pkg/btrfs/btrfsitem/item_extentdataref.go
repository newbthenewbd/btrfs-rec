package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type ExtentDataRef struct { // EXTENT_DATA_REF=178
	Root          int64          `bin:"off=0, siz=8"`
	ObjectID      internal.ObjID `bin:"off=8, siz=8"`
	Offset        int64          `bin:"off=16, siz=8"`
	Count         int32          `bin:"off=24, siz=4"`
	binstruct.End `bin:"off=28"`
}
