// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
)

// A RootRef links subvolumes parent→child for normal subvolumes and
// base→snapshot for snapshot subvolumes.  BACKREF items go the other
// direction; child→parent and snapshot→base.
//
// Key:
//
//	               ROOT_REF                   | ROOT_BACKREF
//	key.objectid = ID of the parent subvolume | ID of the child subvolume
//	key.offset   = ID of the child subvolume  | ID of the parent subvolume
type RootRef struct { // complex ROOT_REF=156 ROOT_BACKREF=144
	DirID         btrfsprim.ObjID `bin:"off=0x00, siz=0x8"` // inode of the parent directory of the dir entry
	Sequence      int64           `bin:"off=0x08, siz=0x8"` // index of that dir entry within the parent
	NameLen       uint16          `bin:"off=0x10, siz=0x2"` // [ignored-when-writing]
	binstruct.End `bin:"off=0x12"`
	Name          []byte `bin:"-"`
}

func (o *RootRef) Free() {
	bytePool.Put(o.Name)
	*o = RootRef{}
	rootRefPool.Put(o)
}

func (o RootRef) Clone() RootRef {
	o.Name = cloneBytes(o.Name)
	return o
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
	o.Name = cloneBytes(dat[n : n+int(o.NameLen)])
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
