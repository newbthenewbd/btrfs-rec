// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"fmt"
	"reflect"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

type Superblock struct {
	Checksum   btrfssum.CSum         `bin:"off=0x0,  siz=0x20"` // Checksum of everything past this field (from 0x20 to 0x1000)
	FSUUID     UUID                  `bin:"off=0x20, siz=0x10"` // FS UUID
	Self       btrfsvol.PhysicalAddr `bin:"off=0x30, siz=0x8"`  // physical address of this block (different for mirrors)
	Flags      uint64                `bin:"off=0x38, siz=0x8"`  // flags
	Magic      [8]byte               `bin:"off=0x40, siz=0x8"`  // magic ('_BHRfS_M')
	Generation Generation            `bin:"off=0x48, siz=0x8"`

	RootTree  btrfsvol.LogicalAddr `bin:"off=0x50, siz=0x8"` // logical address of the root tree root
	ChunkTree btrfsvol.LogicalAddr `bin:"off=0x58, siz=0x8"` // logical address of the chunk tree root
	LogTree   btrfsvol.LogicalAddr `bin:"off=0x60, siz=0x8"` // logical address of the log tree root

	LogRootTransID  uint64 `bin:"off=0x68, siz=0x8"` // log_root_transid
	TotalBytes      uint64 `bin:"off=0x70, siz=0x8"` // total_bytes
	BytesUsed       uint64 `bin:"off=0x78, siz=0x8"` // bytes_used
	RootDirObjectID ObjID  `bin:"off=0x80, siz=0x8"` // root_dir_objectid (usually 6)
	NumDevices      uint64 `bin:"off=0x88, siz=0x8"` // num_devices

	SectorSize        uint32 `bin:"off=0x90, siz=0x4"`
	NodeSize          uint32 `bin:"off=0x94, siz=0x4"`
	LeafSize          uint32 `bin:"off=0x98, siz=0x4"` // unused; must be the same as NodeSize
	StripeSize        uint32 `bin:"off=0x9c, siz=0x4"`
	SysChunkArraySize uint32 `bin:"off=0xa0, siz=0x4"`

	ChunkRootGeneration Generation        `bin:"off=0xa4, siz=0x8"`
	CompatFlags         uint64            `bin:"off=0xac, siz=0x8"` // compat_flags
	CompatROFlags       uint64            `bin:"off=0xb4, siz=0x8"` // compat_ro_flags - only implementations that support the flags can write to the filesystem
	IncompatFlags       IncompatFlags     `bin:"off=0xbc, siz=0x8"` // incompat_flags - only implementations that support the flags can use the filesystem
	ChecksumType        btrfssum.CSumType `bin:"off=0xc4, siz=0x2"`

	RootLevel  uint8 `bin:"off=0xc6, siz=0x1"` // root_level
	ChunkLevel uint8 `bin:"off=0xc7, siz=0x1"` // chunk_root_level
	LogLevel   uint8 `bin:"off=0xc8, siz=0x1"` // log_root_level

	DevItem            btrfsitem.Dev `bin:"off=0xc9,  siz=0x62"`  // DEV_ITEM data for this device
	Label              [0x100]byte   `bin:"off=0x12b, siz=0x100"` // label (may not contain '/' or '\\')
	CacheGeneration    Generation    `bin:"off=0x22b, siz=0x8"`
	UUIDTreeGeneration Generation    `bin:"off=0x233, siz=0x8"`

	// FeatureIncompatMetadataUUID
	MetadataUUID UUID `bin:"off=0x23b, siz=0x10"`

	// FeatureIncompatExtentTreeV2
	NumGlobalRoots uint64 `bin:"off=0x24b, siz=0x8"`

	// FeatureIncompatExtentTreeV2
	BlockGroupRoot           btrfsvol.LogicalAddr `bin:"off=0x253, siz=0x8"`
	BlockGroupRootGeneration Generation           `bin:"off=0x25b, siz=0x8"`
	BlockGroupRootLevel      uint8                `bin:"off=0x263, siz=0x1"`

	Reserved [199]byte `bin:"off=0x264, siz=0xc7"` // future expansion

	SysChunkArray [0x800]byte   `bin:"off=0x32b, siz=0x800"` // sys_chunk_array:(n bytes valid) Contains (KEY . CHUNK_ITEM) pairs for all SYSTEM chunks. This is needed to bootstrap the mapping from logical addresses to physical.
	SuperRoots    [4]RootBackup `bin:"off=0xb2b, siz=0x2a0"`

	// Padded to 4096 bytes
	Padding       [565]byte `bin:"off=0xdcb, siz=0x235"`
	binstruct.End `bin:"off=0x1000"`
}

func (sb Superblock) CalculateChecksum() (btrfssum.CSum, error) {
	data, err := binstruct.Marshal(sb)
	if err != nil {
		return btrfssum.CSum{}, err
	}
	return sb.ChecksumType.Sum(data[binstruct.StaticSize(btrfssum.CSum{}):])
}

func (sb Superblock) ValidateChecksum() error {
	stored := sb.Checksum
	calced, err := sb.CalculateChecksum()
	if err != nil {
		return err
	}
	if calced != stored {
		return fmt.Errorf("superblock checksum mismatch: stored=%v calculated=%v",
			stored, calced)
	}
	return nil
}

func (a Superblock) Equal(b Superblock) bool {
	a.Checksum = btrfssum.CSum{}
	a.Self = 0

	b.Checksum = btrfssum.CSum{}
	b.Self = 0

	return reflect.DeepEqual(a, b)
}

func (sb Superblock) EffectiveMetadataUUID() UUID {
	if !sb.IncompatFlags.Has(FeatureIncompatMetadataUUID) {
		return sb.FSUUID
	}
	return sb.MetadataUUID
}

type SysChunk struct {
	Key   Key
	Chunk btrfsitem.Chunk
}

func (sc SysChunk) MarshalBinary() ([]byte, error) {
	dat, err := binstruct.Marshal(sc.Key)
	if err != nil {
		return dat, err
	}
	_dat, err := binstruct.Marshal(sc.Chunk)
	dat = append(dat, _dat...)
	if err != nil {
		return dat, err
	}
	return dat, nil
}

func (sc *SysChunk) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.Unmarshal(dat, &sc.Key)
	if err != nil {
		return n, err
	}
	_n, err := binstruct.Unmarshal(dat[n:], &sc.Chunk)
	n += _n
	if err != nil {
		return n, err
	}
	return n, nil
}

func (sb Superblock) ParseSysChunkArray() ([]SysChunk, error) {
	dat := sb.SysChunkArray[:sb.SysChunkArraySize]
	var ret []SysChunk
	for len(dat) > 0 {
		var pair SysChunk
		n, err := binstruct.Unmarshal(dat, &pair)
		dat = dat[n:]
		if err != nil {
			return nil, err
		}
		ret = append(ret, pair)
	}
	return ret, nil
}

type RootBackup struct {
	TreeRoot    ObjID      `bin:"off=0x0, siz=0x8"`
	TreeRootGen Generation `bin:"off=0x8, siz=0x8"`

	ChunkRoot    ObjID      `bin:"off=0x10, siz=0x8"`
	ChunkRootGen Generation `bin:"off=0x18, siz=0x8"`

	ExtentRoot    ObjID      `bin:"off=0x20, siz=0x8"`
	ExtentRootGen Generation `bin:"off=0x28, siz=0x8"`

	FSRoot    ObjID      `bin:"off=0x30, siz=0x8"`
	FSRootGen Generation `bin:"off=0x38, siz=0x8"`

	DevRoot    ObjID      `bin:"off=0x40, siz=0x8"`
	DevRootGen Generation `bin:"off=0x48, siz=0x8"`

	ChecksumRoot    ObjID      `bin:"off=0x50, siz=0x8"`
	ChecksumRootGen Generation `bin:"off=0x58, siz=0x8"`

	TotalBytes uint64 `bin:"off=0x60, siz=0x8"`
	BytesUsed  uint64 `bin:"off=0x68, siz=0x8"`
	NumDevices uint64 `bin:"off=0x70, siz=0x8"`

	Unused [8 * 4]byte `bin:"off=0x78, siz=0x20"`

	TreeRootLevel     uint8 `bin:"off=0x98, siz=0x1"`
	ChunkRootLevel    uint8 `bin:"off=0x99, siz=0x1"`
	ExtentRootLevel   uint8 `bin:"off=0x9a, siz=0x1"`
	FSRootLevel       uint8 `bin:"off=0x9b, siz=0x1"`
	DevRootLevel      uint8 `bin:"off=0x9c, siz=0x1"`
	ChecksumRootLevel uint8 `bin:"off=0x9d, siz=0x1"`

	Padding       [10]byte `bin:"off=0x9e, siz=0xa"`
	binstruct.End `bin:"off=0xa8"`
}

type IncompatFlags uint64

const (
	FeatureIncompatMixedBackref = IncompatFlags(1 << iota)
	FeatureIncompatDefaultSubvol
	FeatureIncompatMixedGroups
	FeatureIncompatCompressLZO
	FeatureIncompatCompressZSTD
	FeatureIncompatBigMetadata // buggy
	FeatureIncompatExtendedIRef
	FeatureIncompatRAID56
	FeatureIncompatSkinnyMetadata
	FeatureIncompatNoHoles
	FeatureIncompatMetadataUUID
	FeatureIncompatRAID1C34
	FeatureIncompatZoned
	FeatureIncompatExtentTreeV2
)

var incompatFlagNames = []string{
	"FeatureIncompatMixedBackref",
	"FeatureIncompatDefaultSubvol",
	"FeatureIncompatMixedGroups",
	"FeatureIncompatCompressLZO",
	"FeatureIncompatCompressZSTD",
	"FeatureIncompatBigMetadata ",
	"FeatureIncompatExtendedIRef",
	"FeatureIncompatRAID56",
	"FeatureIncompatSkinnyMetadata",
	"FeatureIncompatNoHoles",
	"FeatureIncompatMetadataUUID",
	"FeatureIncompatRAID1C34",
	"FeatureIncompatZoned",
	"FeatureIncompatExtentTreeV2",
}

func (f IncompatFlags) Has(req IncompatFlags) bool { return f&req == req }
func (f IncompatFlags) String() string {
	return util.BitfieldString(f, incompatFlagNames, util.HexLower)
}
