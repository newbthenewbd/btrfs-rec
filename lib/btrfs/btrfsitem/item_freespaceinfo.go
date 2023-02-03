// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

// key.objectid = object ID of the BlockGroup (logical_addr)
// key.offset = offset of the BlockGroup (size)
type FreeSpaceInfo struct { // trivial FREE_SPACE_INFO=198
	ExtentCount   int32          `bin:"off=0, siz=4"`
	Flags         FreeSpaceFlags `bin:"off=4, siz=4"`
	binstruct.End `bin:"off=8"`
}

type FreeSpaceFlags uint32

const (
	FREE_SPACE_USING_BITMAPS = FreeSpaceFlags(1 << iota)
)

var freeSpaceFlagNames = []string{
	"USING_BITMAPS",
}

func (f FreeSpaceFlags) Has(req FreeSpaceFlags) bool { return f&req == req }
func (f FreeSpaceFlags) String() string {
	return fmtutil.BitfieldString(f, freeSpaceFlagNames, fmtutil.HexNone)
}
