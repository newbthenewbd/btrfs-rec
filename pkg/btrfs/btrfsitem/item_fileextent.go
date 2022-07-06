package btrfsitem

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

// key.objectid = inode
// key.offset = offset within file
type FileExtent struct { // EXTENT_DATA=108
	Generation internal.Generation `bin:"off=0x0, siz=0x8"` // transaction ID that created this extent
	RAMBytes   int64               `bin:"off=0x8, siz=0x8"` // upper bound of what compressed data will decompress to

	// 32 bits describing the data encoding
	Compression   CompressionType `bin:"off=0x10, siz=0x1"`
	Encryption    uint8           `bin:"off=0x11, siz=0x1"`
	OtherEncoding uint16          `bin:"off=0x12, siz=0x2"` // reserved for later use

	Type FileExtentType `bin:"off=0x14, siz=0x1"` // inline data or real extent

	binstruct.End `bin:"off=0x15"`

	// only one of these, depending on .Type
	BodyInline []byte   `bin:"-"` // .Type == FILE_EXTENT_INLINE
	BodyExtent struct { // .Type == FILE_EXTENT_REG or FILE_EXTENT_PREALLOC
		// Position and size of extent within the device
		DiskByteNr   btrfsvol.LogicalAddr `bin:"off=0x0, siz=0x8"`
		DiskNumBytes btrfsvol.AddrDelta   `bin:"off=0x8, siz=0x8"`

		// Position of data within the extent
		Offset btrfsvol.AddrDelta `bin:"off=0x10, siz=0x8"`

		// Decompressed/unencrypted size
		NumBytes int64 `bin:"off=0x18, siz=0x8"`

		binstruct.End `bin:"off=0x20"`
	} `bin:"-"`
}

func (o *FileExtent) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.UnmarshalWithoutInterface(dat, o)
	if err != nil {
		return n, err
	}
	switch o.Type {
	case FILE_EXTENT_INLINE:
		o.BodyInline = dat[n:]
		n += len(o.BodyInline)
	case FILE_EXTENT_REG, FILE_EXTENT_PREALLOC:
		_n, err := binstruct.Unmarshal(dat[n:], &o.BodyExtent)
		n += _n
		if err != nil {
			return n, err
		}
	default:
		return n, fmt.Errorf("unknown file extent type %v", o.Type)
	}
	return n, nil
}

func (o FileExtent) MarshalBinary() ([]byte, error) {
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	switch o.Type {
	case FILE_EXTENT_INLINE:
		dat = append(dat, o.BodyInline...)
	case FILE_EXTENT_REG, FILE_EXTENT_PREALLOC:
		bs, err := binstruct.Marshal(o.BodyExtent)
		dat = append(dat, bs...)
		if err != nil {
			return dat, err
		}
	default:
		return dat, fmt.Errorf("unknown file extent type %v", o.Type)
	}
	return dat, nil
}

type FileExtentType uint8

const (
	FILE_EXTENT_INLINE = FileExtentType(iota)
	FILE_EXTENT_REG
	FILE_EXTENT_PREALLOC
)

func (o FileExtent) Size() (int64, error) {
	switch o.Type {
	case FILE_EXTENT_INLINE:
		return int64(len(o.BodyInline)), nil
	case FILE_EXTENT_REG, FILE_EXTENT_PREALLOC:
		return o.BodyExtent.NumBytes, nil
	default:
		return 0, fmt.Errorf("unknown file extent type %v", o.Type)
	}
}

func (fet FileExtentType) String() string {
	names := map[FileExtentType]string{
		FILE_EXTENT_INLINE:   "inline",
		FILE_EXTENT_REG:      "regular",
		FILE_EXTENT_PREALLOC: "prealloc",
	}
	name, ok := names[fet]
	if !ok {
		name = "unknown"
	}
	return fmt.Sprintf("%d (%s)", fet, name)
}

type CompressionType uint8

const (
	COMPRESS_NONE = CompressionType(iota)
	COMPRESS_ZLIB
	COMPRESS_LZO
	COMPRESS_ZSTD
)

func (ct CompressionType) String() string {
	names := map[CompressionType]string{
		COMPRESS_NONE: "none",
		COMPRESS_ZLIB: "zlib",
		COMPRESS_LZO:  "lzo",
		COMPRESS_ZSTD: "zstd",
	}
	name, ok := names[ct]
	if !ok {
		name = "unknown"
	}
	return fmt.Sprintf("%d (%s)", ct, name)
}
