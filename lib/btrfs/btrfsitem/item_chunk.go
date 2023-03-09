// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

// A Chunk maps logical addresses to physical addresses.
//
// Compare with:
//   - DevExtents, which track allocation of the physical address space.
//   - BlockGroups, which track allocation of the logical address space.
//
// The relationship between the three is
//
//	DevExtent---[many:one]---Chunk---[one:one]---BlockGroup
//
// Key:
//
//	key.objectid = BTRFS_FIRST_CHUNK_TREE_OBJECTID
//	key.offset   = logical_addr
type Chunk struct { // complex CHUNK_ITEM=228
	Head    ChunkHeader
	Stripes []ChunkStripe
}

type ChunkHeader struct {
	Size           btrfsvol.AddrDelta       `bin:"off=0x0,  siz=0x8"`
	Owner          btrfsprim.ObjID          `bin:"off=0x8,  siz=0x8"` // root referencing this chunk (always EXTENT_TREE_OBJECTID=2)
	StripeLen      uint64                   `bin:"off=0x10, siz=0x8"` // ???
	Type           btrfsvol.BlockGroupFlags `bin:"off=0x18, siz=0x8"`
	IOOptimalAlign uint32                   `bin:"off=0x20, siz=0x4"`
	IOOptimalWidth uint32                   `bin:"off=0x24, siz=0x4"`
	IOMinSize      uint32                   `bin:"off=0x28, siz=0x4"` // sector size
	NumStripes     uint16                   `bin:"off=0x2c, siz=0x2"` // [ignored-when-writing]
	SubStripes     uint16                   `bin:"off=0x2e, siz=0x2"` // ???
	binstruct.End  `bin:"off=0x30"`
}

type ChunkStripe struct {
	DeviceID      btrfsvol.DeviceID     `bin:"off=0x0,  siz=0x8"`
	Offset        btrfsvol.PhysicalAddr `bin:"off=0x8,  siz=0x8"`
	DeviceUUID    btrfsprim.UUID        `bin:"off=0x10, siz=0x10"`
	binstruct.End `bin:"off=0x20"`
}

func (chunk Chunk) Mappings(key btrfsprim.Key) []btrfsvol.Mapping {
	ret := make([]btrfsvol.Mapping, 0, len(chunk.Stripes))
	for _, stripe := range chunk.Stripes {
		ret = append(ret, btrfsvol.Mapping{
			LAddr: btrfsvol.LogicalAddr(key.Offset),
			PAddr: btrfsvol.QualifiedPhysicalAddr{
				Dev:  stripe.DeviceID,
				Addr: stripe.Offset,
			},
			Size:       chunk.Head.Size,
			SizeLocked: true,
			Flags:      containers.OptionalValue(chunk.Head.Type),
		})
	}
	return ret
}

var chunkStripePool containers.SlicePool[ChunkStripe]

func (chunk *Chunk) Free() {
	for i := range chunk.Stripes {
		chunk.Stripes[i] = ChunkStripe{}
	}
	chunkStripePool.Put(chunk.Stripes)
	*chunk = Chunk{}
	chunkPool.Put(chunk)
}

func (chunk Chunk) Clone() Chunk {
	ret := chunk
	ret.Stripes = chunkStripePool.Get(len(chunk.Stripes))
	copy(ret.Stripes, chunk.Stripes)
	return ret
}

func (chunk *Chunk) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.Unmarshal(dat, &chunk.Head)
	if err != nil {
		return n, err
	}
	chunk.Stripes = chunkStripePool.Get(int(chunk.Head.NumStripes))
	for i := range chunk.Stripes {
		_n, err := binstruct.Unmarshal(dat[n:], &chunk.Stripes[i])
		n += _n
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (chunk Chunk) MarshalBinary() ([]byte, error) {
	chunk.Head.NumStripes = uint16(len(chunk.Stripes))
	ret, err := binstruct.Marshal(chunk.Head)
	if err != nil {
		return ret, err
	}
	for _, stripe := range chunk.Stripes {
		_ret, err := binstruct.Marshal(stripe)
		ret = append(ret, _ret...)
		if err != nil {
			return ret, err
		}
	}
	return ret, nil
}
