package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type SharedDataRef struct { // SHARED_DATA_REF=184
	Count         int32 `bin:"off=0, siz=4"`
	binstruct.End `bin:"off=4"`
}
