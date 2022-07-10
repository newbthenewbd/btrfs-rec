// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/internal"
)

type ExtentDataRef struct { // EXTENT_DATA_REF=178
	Root          internal.ObjID `bin:"off=0, siz=8"`
	ObjectID      internal.ObjID `bin:"off=8, siz=8"`
	Offset        int64          `bin:"off=16, siz=8"`
	Count         int32          `bin:"off=24, siz=4"`
	binstruct.End `bin:"off=28"`
}
