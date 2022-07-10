// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/internal"
)

// The Key for this item is a UUID, and the item is a subvolume IDs
// that that UUID maps to.
//
// key.objectid = first half of UUID
// key.offset = second half of UUID
type UUIDMap struct { // UUID_SUBVOL=251 UUID_RECEIVED_SUBVOL=252
	ObjID         internal.ObjID `bin:"off=0, siz=8"`
	binstruct.End `bin:"off=8"`
}
