// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
)

// key.objectid = laddr of the extent being referenced
// key.offset = crc32c([root,objectid,offset])
type ExtentDataRef struct { // trivial EXTENT_DATA_REF=178
	Root          btrfsprim.ObjID `bin:"off=0, siz=8"`  // subvolume tree ID that references this extent
	ObjectID      btrfsprim.ObjID `bin:"off=8, siz=8"`  // inode number that references this extent within the .Root subvolume
	Offset        int64           `bin:"off=16, siz=8"` // byte offset for the extent within the file
	Count         int32           `bin:"off=24, siz=4"` // reference count
	binstruct.End `bin:"off=28"`
}
