package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type Empty struct { // UNTYPED=0, ORPHAN_ITEM=48, TREE_BLOCK_REF=176, SHARED_BLOCK_REF=182, FREE_SPACE_EXTENT=199, QGROUP_RELATION=246
	binstruct.End `bin:"off=0"`
}
