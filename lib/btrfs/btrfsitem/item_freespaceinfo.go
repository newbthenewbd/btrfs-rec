// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
)

type FreeSpaceInfo struct { // FREE_SPACE_INFO=198
	ExtentCount   int32  `bin:"off=0, siz=4"`
	Flags         uint32 `bin:"off=4, siz=4"`
	binstruct.End `bin:"off=8"`
}
