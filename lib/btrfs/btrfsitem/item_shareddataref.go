// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
)

// SharedDataRef is part of an Extent.
//
// Key:
//
//	key.objectid = laddr of the extent being referenced
//	key.offset   = laddr of the leaf node containing the FileExtent (EXTENT_DATA_KEY) for this reference.
type SharedDataRef struct { // trivial SHARED_DATA_REF=184
	Count         int32 `bin:"off=0, siz=4"` // reference count
	binstruct.End `bin:"off=4"`
}
