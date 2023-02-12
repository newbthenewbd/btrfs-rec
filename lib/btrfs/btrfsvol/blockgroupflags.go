// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsvol

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

type BlockGroupFlags uint64

const (
	BLOCK_GROUP_DATA = BlockGroupFlags(1 << iota)
	BLOCK_GROUP_SYSTEM
	BLOCK_GROUP_METADATA
	BLOCK_GROUP_RAID0
	BLOCK_GROUP_RAID1
	BLOCK_GROUP_DUP
	BLOCK_GROUP_RAID10
	BLOCK_GROUP_RAID5
	BLOCK_GROUP_RAID6
	BLOCK_GROUP_RAID1C3
	BLOCK_GROUP_RAID1C4

	// BLOCK_GROUP_RAID_MASK is the set of bits that mean that
	// mean the logical:physical relationship is a one:many
	// relationship rather than a one:one relationship.
	//
	// Notably, this does not include BLOCK_GROUP_RAID0.
	BLOCK_GROUP_RAID_MASK = (BLOCK_GROUP_RAID1 | BLOCK_GROUP_DUP | BLOCK_GROUP_RAID10 | BLOCK_GROUP_RAID5 | BLOCK_GROUP_RAID6 | BLOCK_GROUP_RAID1C3 | BLOCK_GROUP_RAID1C4)
)

var blockGroupFlagNames = []string{
	"DATA",
	"SYSTEM",
	"METADATA",

	"RAID0",
	"RAID1",
	"DUP",
	"RAID10",
	"RAID5",
	"RAID6",
	"RAID1C3",
	"RAID1C4",
}

func (f BlockGroupFlags) Has(req BlockGroupFlags) bool { return f&req == req }
func (f BlockGroupFlags) String() string {
	ret := fmtutil.BitfieldString(f, blockGroupFlagNames, fmtutil.HexNone)
	if f&BLOCK_GROUP_RAID_MASK == 0 {
		if ret == "" {
			ret = "single"
		} else {
			ret += "|single"
		}
	}
	return ret
}
