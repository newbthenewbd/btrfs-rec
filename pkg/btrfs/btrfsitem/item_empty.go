package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type Empty struct { // UNTYPED=0, QGROUP_RELATION=246
	binstruct.End `bin:"off=0"`
}
