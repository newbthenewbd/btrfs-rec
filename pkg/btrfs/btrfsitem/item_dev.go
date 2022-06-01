package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type Dev struct { // DEV_ITEM=216
	DeviceID internal.ObjID `bin:"off=0x0,    siz=0x8"` // device ID

	NumBytes     uint64 `bin:"off=0x8,    siz=0x8"` // number of bytes
	NumBytesUsed uint64 `bin:"off=0x10,   siz=0x8"` // number of bytes used

	IOOptimalAlign uint32 `bin:"off=0x18,   siz=0x4"` // optimal I/O align
	IOOptimalWidth uint32 `bin:"off=0x1c,   siz=0x4"` // optimal I/O width
	IOMinSize      uint32 `bin:"off=0x20,   siz=0x4"` // minimal I/O size (sector size)

	Type        uint64              `bin:"off=0x24,   siz=0x8"` // type
	Generation  internal.Generation `bin:"off=0x2c,   siz=0x8"` // generation
	StartOffset uint64              `bin:"off=0x34,   siz=0x8"` // start offset
	DevGroup    uint32              `bin:"off=0x3c,   siz=0x4"` // dev group
	SeekSpeed   uint8               `bin:"off=0x40,   siz=0x1"` // seek speed
	Bandwidth   uint8               `bin:"off=0x41,   siz=0x1"` // bandwidth

	DevUUID internal.UUID `bin:"off=0x42,   siz=0x10"` // device UUID
	FSUUID  internal.UUID `bin:"off=0x52,   siz=0x10"` // FS UUID

	binstruct.End `bin:"off=0x62"`
}
