// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"
	"hash/crc32"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct/binutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
)

const MaxNameLen = 255

func NameHash(dat []byte) uint64 {
	return uint64(^crc32.Update(1, crc32.MakeTable(crc32.Castagnoli), dat))
}

// key.objectid = inode of directory containing this entry
// key.offset =
//   - for DIR_ITEM and XATTR_ITEM = NameHash(name)
//   - for DIR_INDEX               = index id in the directory (starting at 2, because "." and "..")
type DirEntry struct { // complex DIR_ITEM=84 DIR_INDEX=96 XATTR_ITEM=24
	Location      btrfsprim.Key `bin:"off=0x0, siz=0x11"`
	TransID       int64         `bin:"off=0x11, siz=8"`
	DataLen       uint16        `bin:"off=0x19, siz=2"` // [ignored-when-writing]
	NameLen       uint16        `bin:"off=0x1b, siz=2"` // [ignored-when-writing]
	Type          FileType      `bin:"off=0x1d, siz=1"`
	binstruct.End `bin:"off=0x1e"`
	Data          []byte `bin:"-"` // xattr value (only for XATTR_ITEM)
	Name          []byte `bin:"-"`
}

func (o *DirEntry) Free() {
	bytePool.Put(o.Data)
	bytePool.Put(o.Name)
	*o = DirEntry{}
	dirEntryPool.Put(o)
}

func (o DirEntry) Clone() DirEntry {
	o.Data = cloneBytes(o.Data)
	o.Name = cloneBytes(o.Name)
	return o
}

func (o *DirEntry) UnmarshalBinary(dat []byte) (int, error) {
	if err := binutil.NeedNBytes(dat, 0x1e); err != nil {
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
	if err := binutil.NeedNBytes(dat, 0x1e+int(o.DataLen)+int(o.NameLen)); err != nil {
		return 0, err
	}
	o.Name = cloneBytes(dat[n : n+int(o.NameLen)])
	n += int(o.NameLen)
	o.Data = cloneBytes(dat[n : n+int(o.DataLen)])
	n += int(o.DataLen)
	return n, nil
}

func (o DirEntry) MarshalBinary() ([]byte, error) {
	o.DataLen = uint16(len(o.Data))
	o.NameLen = uint16(len(o.Name))
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	dat = append(dat, o.Name...)
	dat = append(dat, o.Data...)
	return dat, nil
}

type FileType uint8

const (
	FT_UNKNOWN FileType = iota
	FT_REG_FILE
	FT_DIR
	FT_CHRDEV
	FT_BLKDEV
	FT_FIFO
	FT_SOCK
	FT_SYMLINK
	FT_XATTR

	FT_MAX
)

var fileTypeNames = []string{
	"UNKNOWN",
	"FILE", // NB: Just "FILE", despite corresponding to "REG_FILE"
	"DIR",
	"CHRDEV",
	"BLKDEV",
	"FIFO",
	"SOCK",
	"SYMLINK",
	"XATTR",
}

func (ft FileType) String() string {
	if ft < FT_MAX {
		return fileTypeNames[ft]
	}
	return fmt.Sprintf("DIR_ITEM.%d", uint8(ft))
}
