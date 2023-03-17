// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

// Inode is a file/dir/whatever in the filesystem.
//
//	key.objectid = inode number
//	key.offset   = 0
type Inode struct { // trivial INODE_ITEM=1
	Generation    btrfsprim.Generation `bin:"off=0x00, siz=0x08"`
	TransID       int64                `bin:"off=0x08, siz=0x08"`
	Size          int64                `bin:"off=0x10, siz=0x08"` // stat
	NumBytes      int64                `bin:"off=0x18, siz=0x08"` // allocated bytes, may be larger than size (or smaller if there are holes?)
	BlockGroup    btrfsprim.ObjID      `bin:"off=0x20, siz=0x08"` // only used for freespace inodes
	NLink         int32                `bin:"off=0x28, siz=0x04"` // stat
	UID           int32                `bin:"off=0x2C, siz=0x04"` // stat
	GID           int32                `bin:"off=0x30, siz=0x04"` // stat
	Mode          StatMode             `bin:"off=0x34, siz=0x04"` // stat
	RDev          int64                `bin:"off=0x38, siz=0x08"` // stat
	Flags         InodeFlags           `bin:"off=0x40, siz=0x08"` // statx.stx_attributes, sorta
	Sequence      int64                `bin:"off=0x48, siz=0x08"` // NFS
	Reserved      [4]int64             `bin:"off=0x50, siz=0x20"`
	ATime         btrfsprim.Time       `bin:"off=0x70, siz=0x0c"` // stat
	CTime         btrfsprim.Time       `bin:"off=0x7c, siz=0x0c"` // stat
	MTime         btrfsprim.Time       `bin:"off=0x88, siz=0x0c"` // stat
	OTime         btrfsprim.Time       `bin:"off=0x94, siz=0x0c"` // statx.stx_btime (why is this called "otime" instead of "btime"?)
	binstruct.End `bin:"off=0xa0"`
}

type InodeFlags uint64

const (
	INODE_NODATASUM InodeFlags = 1 << iota
	INODE_NODATACOW
	INODE_READONLY
	INODE_NOCOMPRESS
	INODE_PREALLOC
	INODE_SYNC
	INODE_IMMUTABLE
	INODE_APPEND
	INODE_NODUMP
	INODE_NOATIME
	INODE_DIRSYNC
	INODE_COMPRESS
)

var inodeFlagNames = []string{
	"NODATASUM",
	"NODATACOW",
	"READONLY",
	"NOCOMPRESS",
	"PREALLOC",
	"SYNC",
	"IMMUTABLE",
	"APPEND",
	"NODUMP",
	"NOATIME",
	"DIRSYNC",
	"COMPRESS",
}

func (f InodeFlags) Has(req InodeFlags) bool { return f&req == req }
func (f InodeFlags) String() string {
	return fmtutil.BitfieldString(f, inodeFlagNames, fmtutil.HexLower)
}
