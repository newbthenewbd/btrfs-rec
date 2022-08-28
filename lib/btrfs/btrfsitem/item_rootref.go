// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
)

type RootRef struct { // ROOT_REF=156 ROOT_BACKREF=144
	DirID         btrfsprim.ObjID `bin:"off=0x00, siz=0x8"`
	Sequence      int64           `bin:"off=0x08, siz=0x8"`
	NameLen       uint16          `bin:"off=0x10, siz=0x2"` // [ignored-when-writing]
	binstruct.End `bin:"off=0x12"`
	Name          []byte `bin:"-"`
}

func (o *RootRef) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 0x12); err != nil {
		return 0, err
	}
	n, err := binstruct.UnmarshalWithoutInterface(dat, o)
	if err != nil {
		return n, err
	}
	if o.NameLen > MaxNameLen {
		return 0, fmt.Errorf("maximum name len is %v, but .NameLen=%v",
			MaxNameLen, o.NameLen)
	}
	if err := binutil.NeedNBytes(dat, 0x12+int(o.NameLen)); err != nil {
		return 0, err
	}
	o.Name = dat[n : n+int(o.NameLen)]
	n += int(o.NameLen)
	return n, nil
}

func (o RootRef) MarshalBinary() ([]byte, error) {
	o.NameLen = uint16(len(o.Name))
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	dat = append(dat, o.Name...)
	return dat, nil
}
