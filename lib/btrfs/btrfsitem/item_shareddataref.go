// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
)

type SharedDataRef struct { // SHARED_DATA_REF=184
	Count         int32 `bin:"off=0, siz=4"`
	binstruct.End `bin:"off=4"`
}
