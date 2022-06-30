package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
	"lukeshu.com/btrfs-tools/pkg/util"
)

// key.objectid = BTRFS_DEV_ITEMS_OBJECTID
// key.offset = device_id (starting at 1)
type Dev struct { // DEV_ITEM=216
	DevID btrfsvol.DeviceID `bin:"off=0x0,    siz=0x8"`

	NumBytes     uint64 `bin:"off=0x8,    siz=0x8"`
	NumBytesUsed uint64 `bin:"off=0x10,   siz=0x8"`

	IOOptimalAlign uint32 `bin:"off=0x18,   siz=0x4"`
	IOOptimalWidth uint32 `bin:"off=0x1c,   siz=0x4"`
	IOMinSize      uint32 `bin:"off=0x20,   siz=0x4"` // sector size

	Type        uint64              `bin:"off=0x24,   siz=0x8"`
	Generation  internal.Generation `bin:"off=0x2c,   siz=0x8"`
	StartOffset uint64              `bin:"off=0x34,   siz=0x8"`
	DevGroup    uint32              `bin:"off=0x3c,   siz=0x4"`
	SeekSpeed   uint8               `bin:"off=0x40,   siz=0x1"`
	Bandwidth   uint8               `bin:"off=0x41,   siz=0x1"`

	DevUUID util.UUID `bin:"off=0x42,   siz=0x10"`
	FSUUID  util.UUID `bin:"off=0x52,   siz=0x10"`

	binstruct.End `bin:"off=0x62"`
}
