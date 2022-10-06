// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsprim

import (
	"fmt"
	"math"
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

var MaxKey = Key{
	ObjectID: math.MaxUint64,
	ItemType: math.MaxUint8,
	Offset:   math.MaxUint64,
}

func (key Key) Mm() Key {
	switch {
	case key.Offset > 0:
		key.Offset--
	case key.ItemType > 0:
		key.ItemType--
	case key.ObjectID > 0:
		key.ObjectID--
	}
	return key
}

func (a Key) Cmp(b Key) int {
	if d := containers.NativeCmp(a.ObjectID, b.ObjectID); d != 0 {
		return d
	}
	if d := containers.NativeCmp(a.ItemType, b.ItemType); d != 0 {
		return d
	}
	return containers.NativeCmp(a.Offset, b.Offset)
}

var _ containers.Ordered[Key] = Key{}

type Time struct {
	Sec           int64  `bin:"off=0x0, siz=0x8"` // Number of seconds since 1970-01-01T00:00:00Z.
	NSec          uint32 `bin:"off=0x8, siz=0x4"` // Number of nanoseconds since the beginning of the second.
	binstruct.End `bin:"off=0xc"`
}

func (t Time) ToStd() time.Time {
	return time.Unix(t.Sec, int64(t.NSec))
}
