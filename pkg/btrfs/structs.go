package btrfs

import (
	"time"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type (
	PhysicalAddr int64
	LogicalAddr  int64
)

type Key struct {
	ObjectID      ObjID  `bin:"off=0, siz=8"` // Object ID. Each tree has its own set of Object IDs.
	ItemType      uint8  `bin:"off=8, siz=1"` // Item type.
	Offset        uint64 `bin:"off=9, siz=8"` // Offset. The meaning depends on the item type.
	binstruct.End `bin:"off=11"`
}

type Time struct {
	Sec           int64  `bin:"off=0, siz=8"` // Number of seconds since 1970-01-01T00:00:00Z.
	NSec          uint64 `bin:"off=8, siz=4"` // Number of nanoseconds since the beginning of the second.
	binstruct.End `bin:"off=c"`
}

func (t Time) ToStd() time.Time {
	return time.Unix(t.Sec, int64(t.NSec))
}

type Superblock struct {
	Checksum   CSum         `bin:"off=0,  siz=20"` // Checksum of everything past this field (from 20 to 1000)
	FSUUID     UUID         `bin:"off=20, siz=10"` // FS UUID
	Self       PhysicalAddr `bin:"off=30, siz=8"`  // physical address of this block (different for mirrors)
	Flags      uint64       `bin:"off=38, siz=8"`  // flags
	Magic      [8]byte      `bin:"off=40, siz=8"`  // magic ('_BHRfS_M')
	Generation uint64       `bin:"off=48, siz=8"`  // generation

	RootTree  LogicalAddr `bin:"off=50, siz=8"` // logical address of the root tree root
	ChunkTree LogicalAddr `bin:"off=58, siz=8"` // logical address of the chunk tree root
	LogTree   LogicalAddr `bin:"off=60, siz=8"` // logical address of the log tree root

	LogRootTransID  uint64 `bin:"off=68, siz=8"` // log_root_transid
	TotalBytes      uint64 `bin:"off=70, siz=8"` // total_bytes
	BytesUsed       uint64 `bin:"off=78, siz=8"` // bytes_used
	RootDirObjectID ObjID  `bin:"off=80, siz=8"` // root_dir_objectid (usually 6)
	NumDevices      uint64 `bin:"off=88, siz=8"` // num_devices

	SectorSize        uint32 `bin:"off=90, siz=4"` // sectorsize
	NodeSize          uint32 `bin:"off=94, siz=4"` // nodesize
	LeafSize          uint32 `bin:"off=98, siz=4"` // leafsize
	StripeSize        uint32 `bin:"off=9c, siz=4"` // stripesize
	SysChunkArraySize uint32 `bin:"off=a0, siz=4"` // sys_chunk_array_size

	ChunkRootGeneration uint64        `bin:"off=a4, siz=8"` // chunk_root_generation
	CompatFlags         uint64        `bin:"off=ac, siz=8"` // compat_flags
	CompatROFlags       uint64        `bin:"off=b4, siz=8"` // compat_ro_flags - only implementations that support the flags can write to the filesystem
	IncompatFlags       IncompatFlags `bin:"off=bc, siz=8"` // incompat_flags - only implementations that support the flags can use the filesystem
	ChecksumType        uint16        `bin:"off=c4, siz=2"` // csum_type - Btrfs currently uses the CRC32c little-endian hash function with seed -1.

	RootLevel  uint8 `bin:"off=c6, siz=1"` // root_level
	ChunkLevel uint8 `bin:"off=c7, siz=1"` // chunk_root_level
	LogLevel   uint8 `bin:"off=c8, siz=1"` // log_root_level

	DevItem            DevItem     `bin:"off=c9,  siz=62"`  // DEV_ITEM data for this device
	Label              [0x100]byte `bin:"off=12b, siz=100"` // label (may not contain '/' or '\\')
	CacheGeneration    uint64      `bin:"off=22b, siz=8"`   // cache_generation
	UUIDTreeGeneration uint64      `bin:"off=233, siz=8"`   // uuid_tree_generation

	// FeatureIncompatMetadataUUID
	MetadataUUID UUID `bin:"off=23b, siz=10"`

	// FeatureIncompatExtentTreeV2
	NumGlobalRoots uint64 `bin:"off=24b, siz=8"`

	// FeatureIncompatExtentTreeV2
	BlockGroupRoot           uint64 `bin:"off=253, siz=8"`
	BlockGroupRootGeneration uint64 `bin:"off=25b, siz=8"`
	BlockGroupRootLevel      uint8  `bin:"off=263, siz=1"`

	Reserved [199]byte `bin:"off=264, siz=c7"` // future expansion

	SysChunkArray  [0x800]byte `bin:"off=32b, siz=800"` // sys_chunk_array:(n bytes valid) Contains (KEY . CHUNK_ITEM) pairs for all SYSTEM chunks. This is needed to bootstrap the mapping from logical addresses to physical.
	TODOSuperRoots [0x2a0]byte `bin:"off=b2b, siz=2a0"` // Contain super_roots (4 btrfs_root_backup)

	// Padded to 4096 bytes
	Padding       [565]byte `bin:"off=dcb, siz=235"`
	binstruct.End `bin:"off=1000"`
}

func (sb Superblock) CalculateChecksum() (CSum, error) {
	data, err := binstruct.Marshal(sb)
	if err != nil {
		return CSum{}, err
	}
	return CRC32c(data[0x20:]), nil
}

func (sb Superblock) EffectiveMetadataUUID() UUID {
	if !sb.IncompatFlags.Has(FeatureIncompatMetadataUUID) {
		return sb.FSUUID
	}
	return sb.MetadataUUID
}

type SysChunk struct {
	Key           `bin:"off=0, siz=11"`
	ChunkItem     `bin:"off=11, siz=30"`
	binstruct.End `bin:"off=41"`
}

func (sb Superblock) ParseSysChunkArray() ([]SysChunk, error) {
	dat := sb.SysChunkArray[:sb.SysChunkArraySize]
	var ret []SysChunk
	for len(dat) > 0 {
		var pair SysChunk
		if err := binstruct.Unmarshal(dat, &pair); err != nil {
			return nil, err
		}
		dat = dat[0x41:]

		for i := 0; i < int(pair.ChunkItem.NumStripes); i++ {
			var stripe ChunkItemStripe
			if err := binstruct.Unmarshal(dat, &stripe); err != nil {
				return nil, err
			}
			pair.ChunkItem.Stripes = append(pair.ChunkItem.Stripes, stripe)
			dat = dat[0x20:]
		}

		ret = append(ret, pair)
	}
	return ret, nil
}

type NodeHeader struct {
	Checksum      CSum        `bin:"off=0,  siz=20"` // Checksum of everything after this field (from 20 to the end of the node)
	MetadataUUID  UUID        `bin:"off=20, siz=10"` // FS UUID
	Addr          LogicalAddr `bin:"off=30, siz=8"`  // Logical address of this node
	Flags         uint64      `bin:"off=38, siz=8"`  // Flags
	ChunkTreeUUID UUID        `bin:"off=40, siz=10"` // Chunk tree UUID
	Generation    uint64      `bin:"off=50, siz=8"`  // Generation
	OwnerTree     TreeObjID   `bin:"off=58, siz=8"`  // The ID of the tree that contains this node
	NumItems      uint32      `bin:"off=60, siz=4"`  // Number of items
	Level         uint8       `bin:"off=64, siz=1"`  // Level (0 for leaf nodes)
	binstruct.End `bin:"off=65"`
}

type InternalNode struct {
	NodeHeader
	Body []KeyPointer
}

type KeyPointer struct {
	Key           Key    `bin:"off=0, siz=11"`
	BlockNumber   uint64 `bin:"off=11, siz=8"`
	Generation    uint64 `bin:"off=19, siz=8"`
	binstruct.End `bin:"off=21"`
}

type LeafNode struct {
	NodeHeader
	Body []Item
}

type Item struct {
	Key           Key    `bin:"off=0, siz=11"`
	DataOffset    uint32 `bin:"off=11, siz=4"` // relative to the end of the header (0x65)
	DataSize      uint32 `bin:"off=15, siz=4"`
	binstruct.End `bin:"off=19"`
}

type DevItem struct {
	DeviceID ObjID `bin:"off=0,    siz=8"` // device ID

	NumBytes     uint64 `bin:"off=8,    siz=8"` // number of bytes
	NumBytesUsed uint64 `bin:"off=10,   siz=8"` // number of bytes used

	IOOptimalAlign uint32 `bin:"off=18,   siz=4"` // optimal I/O align
	IOOptimalWidth uint32 `bin:"off=1c,   siz=4"` // optimal I/O width
	IOMinSize      uint32 `bin:"off=20,   siz=4"` // minimal I/O size (sector size)

	Type        uint64 `bin:"off=24,   siz=8"` // type
	Generation  uint64 `bin:"off=2c,   siz=8"` // generation
	StartOffset uint64 `bin:"off=34,   siz=8"` // start offset
	DevGroup    uint32 `bin:"off=3c,   siz=4"` // dev group
	SeekSpeed   uint8  `bin:"off=40,   siz=1"` // seek speed
	Bandwidth   uint8  `bin:"off=41,   siz=1"` // bandwidth

	DevUUID UUID `bin:"off=42,   siz=10"` // device UUID
	FSUUID  UUID `bin:"off=52,   siz=10"` // FS UUID

	binstruct.End `bin:"off=62"`
}

type ChunkItem struct {
	// Maps logical address to physical.
	Size           uint64 `bin:"off=0,  siz=8"` // size of chunk (bytes)
	Root           ObjID  `bin:"off=8,  siz=8"` // root referencing this chunk (2)
	StripeLen      uint64 `bin:"off=10, siz=8"` // stripe length
	Type           uint64 `bin:"off=18, siz=8"` // type (same as flags for block group?)
	IOOptimalAlign uint32 `bin:"off=20, siz=4"` // optimal io alignment
	IOOptimalWidth uint32 `bin:"off=24, siz=4"` // optimal io width
	IoMinSize      uint32 `bin:"off=28, siz=4"` // minimal io size (sector size)
	NumStripes     uint16 `bin:"off=2c, siz=2"` // number of stripes
	SubStripes     uint16 `bin:"off=2e, siz=2"` // sub stripes
	binstruct.End  `bin:"off=30"`
	Stripes        []ChunkItemStripe `bin:"-"`
}

type ChunkItemStripe struct {
	// Stripes follow (for each number of stripes):
	DeviceID      ObjID  `bin:"off=0,  siz=8"`  // device ID
	Offset        uint64 `bin:"off=8,  siz=8"`  // offset
	DeviceUUID    UUID   `bin:"off=10, siz=10"` // device UUID
	binstruct.End `bin:"off=20"`
}
