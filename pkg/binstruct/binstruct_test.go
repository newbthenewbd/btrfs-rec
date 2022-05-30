package binstruct_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

func TestSmoke(t *testing.T) {
	type UUID [16]byte
	type PhysicalAddr int64
	type DevItem struct {
		DeviceID uint64 `bin:"off=0x0,    siz=0x8"` // device id

		NumBytes     uint64 `bin:"off=0x8,    siz=0x8"` // number of bytes
		NumBytesUsed uint64 `bin:"off=0x10,   siz=0x8"` // number of bytes used

		IOOptimalAlign uint32 `bin:"off=0x18,   siz=0x4"` // optimal I/O align
		IOOptimalWidth uint32 `bin:"off=0x1c,   siz=0x4"` // optimal I/O width
		IOMinSize      uint32 `bin:"off=0x20,   siz=0x4"` // minimal I/O size (sector size)

		Type        uint64 `bin:"off=0x24,   siz=0x8"` // type
		Generation  uint64 `bin:"off=0x2c,   siz=0x8"` // generation
		StartOffset uint64 `bin:"off=0x34,   siz=0x8"` // start offset
		DevGroup    uint32 `bin:"off=0x3c,   siz=0x4"` // dev group
		SeekSpeed   uint8  `bin:"off=0x40,   siz=0x1"` // seek speed
		Bandwidth   uint8  `bin:"off=0x41,   siz=0x1"` // bandwidth

		DevUUID UUID `bin:"off=0x42,   siz=0x10"` // device UUID
		FSUUID  UUID `bin:"off=0x52,   siz=0x10"` // FS UUID

		binstruct.End `bin:"off=0x62"`
	}
	type TestType struct {
		Magic [5]byte      `bin:"off=0x0,siz=0x5"`
		Dev   DevItem      `bin:"off=0x5,siz=0x62"`
		Addr  PhysicalAddr `bin:"off=0x67, siz=0x8"`

		binstruct.End `bin:"off=0x6F"`
	}

	input := TestType{}
	copy(input.Magic[:], "mAgIc")
	input.Dev.DeviceID = 12
	input.Addr = 0xBEEF

	bs, err := binstruct.Marshal(input)
	assert.NoError(t, err)
	assert.True(t, len(bs) == 0x6F, "len(bs)=0x%x", len(bs))

	var output TestType
	n, err := binstruct.Unmarshal(bs, &output)
	assert.NoError(t, err)
	assert.Equal(t, 0x6F, n)
	assert.Equal(t, input, output)
}
