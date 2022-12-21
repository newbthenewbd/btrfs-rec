// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

// key.objectid = logical_addr
// key.offset = size of chunk
type BlockGroup struct { // BLOCK_GROUP_ITEM=192
	Used          int64                    `bin:"off=0, siz=8"`
	ChunkObjectID btrfsprim.ObjID          `bin:"off=8, siz=8"` // always FIRST_CHUNK_TREE_OBJECTID
	Flags         btrfsvol.BlockGroupFlags `bin:"off=16, siz=8"`
	binstruct.End `bin:"off=24"`
}
