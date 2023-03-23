// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

// A Root goes in the ROOT_TREE and defines one of the other trees in
// the filesystem.  All trees have a Root item describing them, except
// for the ROOT_TREE, CHUNK_TREE, TREE_LOG, and BLOCK_GROUP_TREE,
// which are defined directly in the superblock.
//
// Key:
//
//	key.objectid = tree ID
//	key.offset   = one of:
//	   - 0 if objectid is one of the BTRFS_*_TREE_OBJECTID defines or a non-snapshot volume; or
//	   - transaction_id of when this snapshot was created
//
// This tree might contain nodes with node.Head.Owner set to the
// root.ParentUUID tree, *if* the node.Head.Generation is
// less-than-or-equal-to the root's key.offset.  The "or-equal-to"
// part of that might be surprising, which is why I called it out.
type Root struct { // trivial ROOT_ITEM=132
	Inode         Inode                `bin:"off=0x000, siz=0xa0"` // ???
	Generation    btrfsprim.Generation `bin:"off=0x0a0, siz=0x08"`
	RootDirID     btrfsprim.ObjID      `bin:"off=0x0a8, siz=0x08"` // inode number of the root inode
	ByteNr        btrfsvol.LogicalAddr `bin:"off=0x0b0, siz=0x08"` // root node
	ByteLimit     int64                `bin:"off=0x0b8, siz=0x08"` // always 0 (unused)
	BytesUsed     int64                `bin:"off=0x0c0, siz=0x08"`
	LastSnapshot  int64                `bin:"off=0x0c8, siz=0x08"`
	Flags         RootFlags            `bin:"off=0x0d0, siz=0x08"`
	Refs          int32                `bin:"off=0x0d8, siz=0x04"`
	DropProgress  btrfsprim.Key        `bin:"off=0x0dc, siz=0x11"`
	DropLevel     uint8                `bin:"off=0x0ed, siz=0x01"`
	Level         uint8                `bin:"off=0x0ee, siz=0x01"`
	GenerationV2  btrfsprim.Generation `bin:"off=0x0ef, siz=0x08"`
	UUID          btrfsprim.UUID       `bin:"off=0x0f7, siz=0x10"`
	ParentUUID    btrfsprim.UUID       `bin:"off=0x107, siz=0x10"`
	ReceivedUUID  btrfsprim.UUID       `bin:"off=0x117, siz=0x10"`
	CTransID      int64                `bin:"off=0x127, siz=0x08"`
	OTransID      int64                `bin:"off=0x12f, siz=0x08"`
	STransID      int64                `bin:"off=0x137, siz=0x08"`
	RTransID      int64                `bin:"off=0x13f, siz=0x08"`
	CTime         btrfsprim.Time       `bin:"off=0x147, siz=0x0c"`
	OTime         btrfsprim.Time       `bin:"off=0x153, siz=0x0c"`
	STime         btrfsprim.Time       `bin:"off=0x15f, siz=0x0c"`
	RTime         btrfsprim.Time       `bin:"off=0x16b, siz=0x0c"`
	GlobalTreeID  btrfsprim.ObjID      `bin:"off=0x177, siz=0x08"` // ???
	Reserved      [7]int64             `bin:"off=0x17f, siz=0x38"`
	binstruct.End `bin:"off=0x1b7"`
}

type RootFlags uint64

const (
	ROOT_SUBVOL_RDONLY RootFlags = 1 << iota
)

var rootFlagNames = []string{
	"SUBVOL_RDONLY",
}

func (f RootFlags) Has(req RootFlags) bool { return f&req == req }
func (f RootFlags) String() string         { return fmtutil.BitfieldString(f, rootFlagNames, fmtutil.HexLower) }
