// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package btrfsitem contains the definitions of all "items" that may
// be stored in a btrfs tree.
package btrfsitem

import (
	"fmt"
	"reflect"

	"git.lukeshu.com/go/typedsync"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type Type = btrfsprim.ItemType

type Item interface {
	isItem()
	Free()
	CloneItem() Item
}

type Error struct {
	Dat []byte
	Err error
}

var errorPool = &typedsync.Pool[*Error]{New: func() *Error { return new(Error) }}

func (*Error) isItem() {}

func (o *Error) Free() {
	*o = Error{}
	errorPool.Put(o)
}

func (o Error) Clone() Error { return o }

func (o *Error) CloneItem() Item {
	ret, _ := errorPool.Get()
	*ret = *o
	return ret
}

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
			ret, _ := errorPool.Get()
			*ret = Error{
				Dat: dat,
				Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v, ObjectID:%v}, dat): unknown object ID for untyped item",
					key.ItemType, key.ObjectID),
			}
			return ret
		}
	} else {
		var ok bool
		gotyp, ok = keytype2gotype[key.ItemType]
		if !ok {
			ret, _ := errorPool.Get()
			*ret = Error{
				Dat: dat,
				Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v}, dat): unknown item type", key.ItemType),
			}
			return ret
		}
	}
	ptr, _ := gotype2pool[gotyp].Get()
	if csums, ok := ptr.(*ExtentCSum); ok {
		csums.ChecksumSize = csumType.Size()
		csums.Addr = btrfsvol.LogicalAddr(key.Offset)
	}
	n, err := binstruct.Unmarshal(dat, ptr)
	if err != nil {
		ptr.Free()
		ret, _ := errorPool.Get()
		*ret = Error{
			Dat: dat,
			Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v}, dat): %w", key.ItemType, err),
		}
		return ret
	}
	if n < len(dat) {
		ptr.Free()
		ret, _ := errorPool.Get()
		*ret = Error{
			Dat: dat,
			Err: fmt.Errorf("btrfsitem.UnmarshalItem({ItemType:%v}, dat): left over data: got %v bytes but only consumed %v",
				key.ItemType, len(dat), n),
		}
		return ret
	}
	return ptr
}

var bytePool containers.SlicePool[byte]

func cloneBytes(in []byte) []byte {
	out := bytePool.Get(len(in))
	copy(out, in)
	return out
}
