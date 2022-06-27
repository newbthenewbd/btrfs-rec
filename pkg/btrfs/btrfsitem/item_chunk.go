package btrfsitem

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
	"lukeshu.com/btrfs-tools/pkg/util"
)

// Maps logical address to physical.
//
// key.objectid = BTRFS_FIRST_CHUNK_TREE_OBJECTID
// key.offset = logical_addr
type Chunk struct { // CHUNK_ITEM=228
	Head    ChunkHeader
	Stripes []ChunkStripe
}

type ChunkHeader struct {
	Size           btrfsvol.AddrDelta       `bin:"off=0x0,  siz=0x8"`
	Owner          internal.ObjID           `bin:"off=0x8,  siz=0x8"` // root referencing this chunk (always EXTENT_TREE_OBJECTID=2)
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
	DeviceUUID    util.UUID             `bin:"off=0x10, siz=0x10"`
	binstruct.End `bin:"off=0x20"`
}

func (chunk Chunk) Mappings(key internal.Key) []btrfsvol.Mapping {
	ret := make([]btrfsvol.Mapping, 0, len(chunk.Stripes))
	for _, stripe := range chunk.Stripes {
		ret = append(ret, btrfsvol.Mapping{
			LAddr: btrfsvol.LogicalAddr(key.Offset),
			PAddr: btrfsvol.QualifiedPhysicalAddr{
				Dev:  stripe.DeviceID,
				Addr: stripe.Offset,
			},
			Size:  chunk.Head.Size,
			Flags: &chunk.Head.Type,
		})
	}
	return ret
}

func (chunk *Chunk) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.Unmarshal(dat, &chunk.Head)
	if err != nil {
		return n, err
	}
	chunk.Stripes = nil
	for i := 0; i < int(chunk.Head.NumStripes); i++ {
		var stripe ChunkStripe
		_n, err := binstruct.Unmarshal(dat[n:], &stripe)
		n += _n
		if err != nil {
			return n, fmt.Errorf("%T.UnmarshalBinary: %w", *chunk, err)
		}
		chunk.Stripes = append(chunk.Stripes, stripe)
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
			return ret, fmt.Errorf("%T.MarshalBinary: %w", chunk, err)
		}
	}
	return ret, nil
}
