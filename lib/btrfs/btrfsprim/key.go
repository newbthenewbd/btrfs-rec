// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsprim

import (
	"fmt"
	"math"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type Key struct {
	ObjectID      ObjID    `bin:"off=0x0, siz=0x8"` // Each tree has its own set of Object IDs.
	ItemType      ItemType `bin:"off=0x8, siz=0x1"`
	Offset        uint64   `bin:"off=0x9, siz=0x8"` // The meaning depends on the item type.
	binstruct.End `bin:"off=0x11"`
}

const MaxOffset uint64 = math.MaxUint64

// mimics print-tree.c:btrfs_print_key()
func (key Key) Format(tree ObjID) string {
	switch tree {
	case UUID_TREE_OBJECTID:
		return fmt.Sprintf("(%v %v %#08x)",
			key.ObjectID.Format(tree),
			key.ItemType,
			key.Offset)
	case ROOT_TREE_OBJECTID, QUOTA_TREE_OBJECTID:
		return fmt.Sprintf("(%v %v %v)",
			key.ObjectID.Format(tree),
			key.ItemType,
			ObjID(key.Offset).Format(tree))
	default:
		if key.Offset == math.MaxUint64 {
			return fmt.Sprintf("(%v %v -1)",
				key.ObjectID.Format(tree),
				key.ItemType)
		} else {
			return fmt.Sprintf("(%v %v %v)",
				key.ObjectID.Format(tree),
				key.ItemType,
				key.Offset)
		}
	}
}

func (key Key) String() string {
	return key.Format(0)
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
		key.Offset = MaxOffset
	case key.ObjectID > 0:
		key.ObjectID--
		key.ItemType = MAX_KEY
		key.Offset = MaxOffset
	}
	return key
}

func (key Key) Pp() Key {
	switch {
	case key.Offset < MaxOffset:
		key.Offset++
	case key.ItemType < MAX_KEY:
		key.ItemType++
		key.Offset = 0
	case key.ObjectID < MAX_OBJECTID:
		key.ObjectID++
		key.ItemType = 0
		key.Offset = 0
	}
	return key
}

func (a Key) Compare(b Key) int {
	if d := containers.NativeCompare(a.ObjectID, b.ObjectID); d != 0 {
		return d
	}
	if d := containers.NativeCompare(a.ItemType, b.ItemType); d != 0 {
		return d
	}
	return containers.NativeCompare(a.Offset, b.Offset)
}

var _ containers.Ordered[Key] = Key{}
