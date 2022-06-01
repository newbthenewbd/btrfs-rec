package btrfsitem

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type Chunk struct { // CHUNK_ITEM=228
	// Maps logical address to physical.
	Size           uint64         `bin:"off=0x0,  siz=0x8"` // size of chunk (bytes)
	Owner          internal.ObjID `bin:"off=0x8,  siz=0x8"` // root referencing this chunk (2)
	StripeLen      uint64         `bin:"off=0x10, siz=0x8"` // stripe length
	Type           uint64         `bin:"off=0x18, siz=0x8"` // type (same as flags for block group?)
	IOOptimalAlign uint32         `bin:"off=0x20, siz=0x4"` // optimal io alignment
	IOOptimalWidth uint32         `bin:"off=0x24, siz=0x4"` // optimal io width
	IoMinSize      uint32         `bin:"off=0x28, siz=0x4"` // minimal io size (sector size)
	NumStripes     uint16         `bin:"off=0x2c, siz=0x2"` // number of stripes
	SubStripes     uint16         `bin:"off=0x2e, siz=0x2"` // sub stripes
	binstruct.End  `bin:"off=0x30"`
	Stripes        []ChunkStripe `bin:"-"`
}

type ChunkStripe struct {
	DeviceID      internal.ObjID `bin:"off=0x0,  siz=0x8"`
	Offset        uint64         `bin:"off=0x8,  siz=0x8"`
	DeviceUUID    internal.UUID  `bin:"off=0x10, siz=0x10"`
	binstruct.End `bin:"off=0x20"`
}

func (chunk *Chunk) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.UnmarshalWithoutInterface(dat, chunk)
	if err != nil {
		return n, err
	}
	for i := 0; i < int(chunk.NumStripes); i++ {
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
	chunk.NumStripes = uint16(len(chunk.Stripes))
	ret, err := binstruct.MarshalWithoutInterface(chunk)
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
