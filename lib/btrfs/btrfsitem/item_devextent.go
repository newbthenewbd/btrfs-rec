// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

// key.objectid = device_id
// key.offset = physical_addr
type DevExtent struct { // DEV_EXTENT=204
	ChunkTree     btrfsprim.ObjID      `bin:"off=0, siz=8"`  // always CHUNK_TREE_OBJECTID
	ChunkObjectID btrfsprim.ObjID      `bin:"off=8, siz=8"`  // which chunk within .ChunkTree owns this extent, always FIRST_CHUNK_TREE_OBJECTID
	ChunkOffset   btrfsvol.LogicalAddr `bin:"off=16, siz=8"` // offset of the CHUNK_ITEM that owns this extent, within the .ChunkObjectID
	Length        btrfsvol.AddrDelta   `bin:"off=24, siz=8"`
	ChunkTreeUUID btrfsprim.UUID       `bin:"off=32, siz=16"`
	binstruct.End `bin:"off=48"`
}

func (devext DevExtent) Mapping(key btrfsprim.Key) btrfsvol.Mapping {
	return btrfsvol.Mapping{
		LAddr: devext.ChunkOffset,
		PAddr: btrfsvol.QualifiedPhysicalAddr{
			Dev:  btrfsvol.DeviceID(key.ObjectID),
			Addr: btrfsvol.PhysicalAddr(key.Offset),
		},
		Size:       devext.Length,
		SizeLocked: true,
	}
}
