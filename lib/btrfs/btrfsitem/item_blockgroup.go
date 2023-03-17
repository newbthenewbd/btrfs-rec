// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

// A BlockGroup tracks allocation of the logical address space.
//
// Compare with:
//   - DevExtents, which track allocation of the physical address space.
//   - Chunks, which map logical addresses to physical addresses.
//
// The relationship between the three is
//
//	DevExtent---[many:one]---Chunk---[one:one]---BlockGroup
//
// Key:
//
//	key.objectid = logical_addr
//	key.offset   = size of chunk
type BlockGroup struct { // trivial BLOCK_GROUP_ITEM=192
	Used          int64                    `bin:"off=0, siz=8"`
	ChunkObjectID btrfsprim.ObjID          `bin:"off=8, siz=8"` // always FIRST_CHUNK_TREE_OBJECTID
	Flags         btrfsvol.BlockGroupFlags `bin:"off=16, siz=8"`
	binstruct.End `bin:"off=24"`
}
