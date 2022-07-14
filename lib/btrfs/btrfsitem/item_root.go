// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/internal"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

type Root struct { // ROOT_ITEM=132
	Inode         Inode                `bin:"off=0x000, siz=0xa0"`
	Generation    internal.Generation  `bin:"off=0x0a0, siz=0x08"`
	RootDirID     internal.ObjID       `bin:"off=0x0a8, siz=0x08"`
	ByteNr        btrfsvol.LogicalAddr `bin:"off=0x0b0, siz=0x08"`
	ByteLimit     int64                `bin:"off=0x0b8, siz=0x08"`
	BytesUsed     int64                `bin:"off=0x0c0, siz=0x08"`
	LastSnapshot  int64                `bin:"off=0x0c8, siz=0x08"`
	Flags         RootFlags            `bin:"off=0x0d0, siz=0x08"`
	Refs          int32                `bin:"off=0x0d8, siz=0x04"`
	DropProgress  internal.Key         `bin:"off=0x0dc, siz=0x11"`
	DropLevel     uint8                `bin:"off=0x0ed, siz=0x01"`
	Level         uint8                `bin:"off=0x0ee, siz=0x01"`
	GenerationV2  internal.Generation  `bin:"off=0x0ef, siz=0x08"`
	UUID          internal.UUID        `bin:"off=0x0f7, siz=0x10"`
	ParentUUID    internal.UUID        `bin:"off=0x107, siz=0x10"`
	ReceivedUUID  internal.UUID        `bin:"off=0x117, siz=0x10"`
	CTransID      int64                `bin:"off=0x127, siz=0x08"`
	OTransID      int64                `bin:"off=0x12f, siz=0x08"`
	STransID      int64                `bin:"off=0x137, siz=0x08"`
	RTransID      int64                `bin:"off=0x13f, siz=0x08"`
	CTime         internal.Time        `bin:"off=0x147, siz=0x0c"`
	OTime         internal.Time        `bin:"off=0x153, siz=0x0c"`
	STime         internal.Time        `bin:"off=0x15f, siz=0x0c"`
	RTime         internal.Time        `bin:"off=0x16b, siz=0x0c"`
	GlobalTreeID  internal.ObjID       `bin:"off=0x177, siz=0x08"`
	Reserved      [7]int64             `bin:"off=0x17f, siz=0x38"`
	binstruct.End `bin:"off=0x1b7"`
}

type RootFlags uint64

const (
	ROOT_SUBVOL_RDONLY = RootFlags(1 << iota)
)

var rootFlagNames = []string{
	"SUBVOL_RDONLY",
}

func (f RootFlags) Has(req RootFlags) bool { return f&req == req }
func (f RootFlags) String() string         { return fmtutil.BitfieldString(f, rootFlagNames, fmtutil.HexLower) }
