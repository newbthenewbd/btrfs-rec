// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"
	"reflect"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type Type = btrfsprim.ItemType

type Item interface {
	isItem()
}

type Error struct {
	Dat []byte
	Err error
}

func (*Error) isItem() {}

func (o Error) MarshalBinary() ([]byte, error) {
	return o.Dat, nil
}

func (o *Error) UnmarshalBinary(dat []byte) (int, error) {
	o.Dat = dat
	return len(dat), nil
}

// Rather than returning a separate error value, return an Error item.
func UnmarshalItem(key btrfsprim.Key, csumType btrfssum.CSumType, dat []byte) Item {
	var gotyp reflect.Type
	if key.ItemType == UNTYPED_KEY {
		var ok bool
		gotyp, ok = untypedObjID2gotype[key.ObjectID]
		if !ok {
			return &Error{
				Dat: dat,
				Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v, ObjectID:%v}, dat): unknown object ID for untyped item",
					key.ItemType, key.ObjectID),
			}
		}
	} else {
		var ok bool
		gotyp, ok = keytype2gotype[key.ItemType]
		if !ok {
			return &Error{
				Dat: dat,
				Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v}, dat): unknown item type", key.ItemType),
			}
		}
	}
	ptr := reflect.New(gotyp)
	if csums, ok := ptr.Interface().(*ExtentCSum); ok {
		csums.ChecksumSize = csumType.Size()
		csums.Addr = btrfsvol.LogicalAddr(key.Offset)
	}
	n, err := binstruct.Unmarshal(dat, ptr.Interface())
	if err != nil {
		return &Error{
			Dat: dat,
			Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v}, dat): %w", key.ItemType, err),
		}
	}
	if n < len(dat) {
		return &Error{
			Dat: dat,
			Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v}, dat): left over data: got %v bytes but only consumed %v",
				key.ItemType, len(dat), n),
		}
	}
	//nolint:forcetypeassert // items_gen.go has all types in keytype2gotype implement the Item interface.
	return ptr.Interface().(Item)
}
