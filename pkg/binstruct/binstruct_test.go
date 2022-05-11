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
		DeviceID uint64 `bin:"off=0,    siz=8,  desc=device id"`

		NumBytes     uint64 `bin:"off=8,    siz=8,  desc=number of bytes"`
		NumBytesUsed uint64 `bin:"off=10,   siz=8,  desc=number of bytes used"`

		IOOptimalAlign uint32 `bin:"off=18,   siz=4,  desc=optimal I/O align"`
		IOOptimalWidth uint32 `bin:"off=1c,   siz=4,  desc=optimal I/O width"`
		IOMinSize      uint32 `bin:"off=20,   siz=4,  desc=minimal I/O size (sector size)"`

		Type        uint64 `bin:"off=24,   siz=8,  desc=type"`
		Generation  uint64 `bin:"off=2c,   siz=8,  desc=generation"`
		StartOffset uint64 `bin:"off=34,   siz=8,  desc=start offset"`
		DevGroup    uint32 `bin:"off=3c,   siz=4,  desc=dev group"`
		SeekSpeed   uint8  `bin:"off=40,   siz=1,  desc=seek speed"`
		Bandwidth   uint8  `bin:"off=41,   siz=1,  desc=bandwidth"`

		DevUUID UUID `bin:"off=42,   siz=10,         desc=device UUID"`
		FSUUID  UUID `bin:"off=52,   siz=10,         desc=FS UUID"`

		binstruct.End `bin:"off=62"`
	}
	type TestType struct {
		Magic [5]byte      `bin:"off=0,siz=5"`
		Dev   DevItem      `bin:"off=5,siz=62"`
		Addr  PhysicalAddr `bin:"off=67, siz=8"`

		binstruct.End `bin:"off=6F"`
	}

	input := TestType{}
	copy(input.Magic[:], "mAgIc")
	input.Dev.DeviceID = 12
	input.Addr = 0xBEEF

	bs, err := binstruct.Marshal(input)
	assert.NoError(t, err)
	assert.True(t, len(bs) == 0x6F, "len(bs)=0x%x", len(bs))

	var output TestType
	assert.NoError(t, binstruct.Unmarshal(bs, &output))
	assert.Equal(t, input, output)
}
