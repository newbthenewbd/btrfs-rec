// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
)

// QGroupInfo tracks the amount of space used by a given qgroup in the
// containing subvolume.
//
// Key:
//
//	key.objectid = 0
//	key.offset   = ID of the qgroup
type QGroupInfo struct { // trivial QGROUP_INFO=242
	Generation                btrfsprim.Generation `bin:"off=0, siz=8"`
	ReferencedBytes           uint64               `bin:"off=8, siz=8"`
	ReferencedBytesCompressed uint64               `bin:"off=16, siz=8"`
	ExclusiveBytes            uint64               `bin:"off=24, siz=8"`
	ExclusiveBytesCompressed  uint64               `bin:"off=32, siz=8"`
	binstruct.End             `bin:"off=40"`
}
