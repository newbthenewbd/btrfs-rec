// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package internal

import (
	"fmt"
	"time"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type Generation uint64

type Key struct {
	ObjectID      ObjID    `bin:"off=0x0, siz=0x8"` // Each tree has its own set of Object IDs.
	ItemType      ItemType `bin:"off=0x8, siz=0x1"`
	Offset        uint64   `bin:"off=0x9, siz=0x8"` // The meaning depends on the item type.
	binstruct.End `bin:"off=0x11"`
}

func (k Key) String() string {
	return fmt.Sprintf("{%v %v %v}", k.ObjectID, k.ItemType, k.Offset)
}

func (a Key) Cmp(b Key) int {
	if d := containers.CmpUint(a.ObjectID, b.ObjectID); d != 0 {
		return d
	}
	if d := containers.CmpUint(a.ItemType, b.ItemType); d != 0 {
		return d
	}
	return containers.CmpUint(a.Offset, b.Offset)
}

type Time struct {
	Sec           int64  `bin:"off=0x0, siz=0x8"` // Number of seconds since 1970-01-01T00:00:00Z.
	NSec          uint32 `bin:"off=0x8, siz=0x4"` // Number of nanoseconds since the beginning of the second.
	binstruct.End `bin:"off=0xc"`
}

func (t Time) ToStd() time.Time {
	return time.Unix(t.Sec, int64(t.NSec))
}
