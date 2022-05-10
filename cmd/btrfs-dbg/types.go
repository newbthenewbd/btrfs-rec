package main

import (
	"encoding/hex"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type (
	PhysicalAddr int64
	LogicalAddr  int64
	ObjID        int64
	UUID         [16]byte
)

func (uuid UUID) String() string {
	str := hex.EncodeToString(uuid[:])
	return strings.Join([]string{
		str[:8],
		str[8:12],
		str[12:16],
		str[16:20],
		str[20:32],
	}, "-")
}

type Superblock struct {
	Checksum   [0x20]byte   `bin:"off=0,  siz=20, desc=Checksum of everything past this field (from 20 to 1000)"`
	FSUUID     UUID         `bin:"off=20, siz=10, desc=FS UUID"`
	Self       PhysicalAddr `bin:"off=30, siz=8,  desc=physical address of this block (different for mirrors)"`
	Flags      uint64       `bin:"off=38, siz=8,  desc=flags"`
	Magic      [8]byte      `bin:"off=40, siz=8,  desc=magic ('_BHRfS_M')"`
	Generation uint64       `bin:"off=48, siz=8,  desc=generation"`

	RootTree  LogicalAddr `bin:"off=50, siz=8,  desc=logical address of the root tree root"`
	ChunkTree LogicalAddr `bin:"off=58, siz=8,  desc=logical address of the chunk tree root"`
	LogTree   LogicalAddr `bin:"off=60, siz=8,  desc=logical address of the log tree root"`

	LogRootTransID  uint64 `bin:"off=68, siz=8, desc=log_root_transid"`
	TotalBytes      uint64 `bin:"off=70, siz=8, desc=total_bytes"`
	BytesUsed       uint64 `bin:"off=78, siz=8, desc=bytes_used"`
	RootDirObjectID uint64 `bin:"off=80, siz=8, desc=root_dir_objectid (usually 6)"`
	NumDevices      uint64 `bin:"off=88, siz=8, desc=num_devices"`

	SectorSize        uint32 `bin:"off=90, siz=4, desc=sectorsize"`
	NodeSize          uint32 `bin:"off=94, siz=4, desc=nodesize"`
	LeafSize          uint32 `bin:"off=98, siz=4, desc=leafsize"`
	StripeSize        uint32 `bin:"off=9c, siz=4, desc=stripesize"`
	SysChunkArraySize uint32 `bin:"off=a0, siz=4, desc=sys_chunk_array_size"`

	ChunkRootGeneration uint64 `bin:"off=a4, siz=8, desc=chunk_root_generation"`
	CompatFlags         uint64 `bin:"off=ac, siz=8, desc=compat_flags"`
	CompatROFlags       uint64 `bin:"off=b4, siz=8, desc=compat_ro_flags - only implementations that support the flags can write to the filesystem"`
	IncompatFlags       uint64 `bin:"off=bc, siz=8, desc=incompat_flags - only implementations that support the flags can use the filesystem"`
	ChecksumType        uint16 `bin:"off=c4, siz=2, desc=csum_type - Btrfs currently uses the CRC32c little-endian hash function with seed -1."`

	RootLevel  uint8 `bin:"off=c6, siz=1, desc=root_level"`
	ChunkLevel uint8 `bin:"off=c7, siz=1, desc=chunk_root_level"`
	LogLevel   uint8 `bin:"off=c8, siz=1, desc=log_root_level"`

	DevItem            DevItem     `bin:"off=c9,  siz=62,  desc=DEV_ITEM data for this device"`
	Label              [0x100]byte `bin:"off=12b, siz=100, desc=label (may not contain '/' or '\\')"`
	CacheGeneration    uint64      `bin:"off=22b, siz=8,   desc=cache_generation"`
	UUIDTreeGeneration uint64      `bin:"off=233, siz=8,   desc=uuid_tree_generation"`

	Reserved [0xf0]byte `bin:"off=23b, siz=f0,  desc=reserved /* future expansion */"`

	TODOSysChunkArray [0x800]byte `bin:"off=32b, siz=800, desc=sys_chunk_array:(n bytes valid) Contains (KEY . CHUNK_ITEM) pairs for all SYSTEM chunks. This is needed to bootstrap the mapping from logical addresses to physical. "`
	TODOSuperRoots    [0x2a0]byte `bin:"off=b2b, siz=2a0, desc=Contain super_roots (4 btrfs_root_backup)"`

	Unused [0x235]byte `bin:"off=dcb, siz=235, desc=current unused"`

	binstruct.End `bin:"off=1000"`
}

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

type ChunkItem struct {
	// Maps logical address to physical.
	Size           uint64 `bin:"off=0,  siz=8, desc=size of chunk (bytes)"`
	Root           ObjID  `bin:"off=8,  siz=8, desc=root referencing this chunk (2)"`
	StripeLen      uint64 `bin:"off=10, siz=8, desc=stripe length"`
	Type           uint64 `bin:"off=18, siz=8, desc=type (same as flags for block group?)"`
	IOOptimalAlign uint32 `bin:"off=20, siz=4, desc=optimal io alignment"`
	IOOptimalWidth uint32 `bin:"off=24, siz=4, desc=optimal io width"`
	IoMinSize      uint32 `bin:"off=28, siz=4, desc=minimal io size (sector size)"`
	NumStripes     uint16 `bin:"off=2c, siz=2, desc=number of stripes"`
	SubStripes     uint16 `bin:"off=2e, siz=2, desc=sub stripes"`
	binstruct.End  `bin:"off=30"`
}

type ChunkItemStripe struct {
	// Stripes follow (for each number of stripes):
	DeviceID      ObjID  `bin:"off=0,  siz=8,  desc=device id"`
	Offset        uint64 `bin:"off=8,  siz=8,  desc=offset"`
	DeviceUUID    UUID   `bin:"off=10, siz=10, desc=device UUID"`
	binstruct.End `bin:"off=20"`
}
