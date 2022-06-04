package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type Empty struct { // UNTYPED=0, TREE_BLOCK_REF=176, SHARED_BLOCK_REF=182, QGROUP_RELATION=246
	binstruct.End `bin:"off=0"`
}
