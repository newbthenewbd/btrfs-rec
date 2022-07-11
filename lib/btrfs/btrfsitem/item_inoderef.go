// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binutil"
)

// key.objectid = inode number of the file
// key.offset = inode number of the parent file
type InodeRef struct { // INODE_REF=12
	Index         int64  `bin:"off=0x0, siz=0x8"`
	NameLen       uint16 `bin:"off=0x8, siz=0x2"` // [ignored-when-writing]
	binstruct.End `bin:"off=0xa"`
	Name          []byte `bin:"-"`
}

func (o *InodeRef) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 0xA); err != nil {
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
	if err := binutil.NeedNBytes(dat, 0xA+int(o.NameLen)); err != nil {
		return 0, err
	}
	dat = dat[n:]
	o.Name = dat[:o.NameLen]
	n += int(o.NameLen)
	return n, nil
}

func (o InodeRef) MarshalBinary() ([]byte, error) {
	o.NameLen = uint16(len(o.Name))
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	dat = append(dat, o.Name...)
	return dat, nil
}
