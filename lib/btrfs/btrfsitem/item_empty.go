// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
)

type Empty struct { // trivial ORPHAN_ITEM=48 TREE_BLOCK_REF=176 SHARED_BLOCK_REF=182 FREE_SPACE_EXTENT=199 QGROUP_RELATION=246
	binstruct.End `bin:"off=0"`
}
