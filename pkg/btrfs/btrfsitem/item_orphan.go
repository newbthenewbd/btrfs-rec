package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type Orphan struct { // ORPHAN_ITEM=48
	binstruct.End `bin:"off=0"`
}
