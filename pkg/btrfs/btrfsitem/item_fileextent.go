package btrfsitem

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type FileExtent struct { // EXTENT_DATA=108
	Generation int64 `bin:"off=0x0, siz=0x8"` // transaction ID that created this extent
	RAMBytes   int64 `bin:"off=0x8, siz=0x8"` // upper bound of what compressed data will decompress to

	// 32 bits describing the data encoding
	Compression   CompressionType `bin:"off=0x10, siz=0x1"`
	Encryption    uint8           `bin:"off=0x11, siz=0x1"`
	OtherEncoding uint16          `bin:"off=0x12, siz=0x2"` // reserved for later use

	Type FileExtentType `bin:"off=0x14, siz=0x1"` // inline data or real extent

	binstruct.End `bin:"off=0x15"`

	// only one of these, depending on .Type
	BodyInline []byte `bin:"-"`
	BodyReg    struct {
		// Position within the device
		DiskByteNr   int64 `bin:"off=0x0, siz=0x8"`
		DiskNumBytes int64 `bin:"off=0x8, siz=0x8"`

		// Position within the file
		Offset        int64 `bin:"off=0x10, siz=0x8"`
		NumBytes      int64 `bin:"off=0x18, siz=0x8"`
		binstruct.End `bin:"off=0x20"`
	} `bin:"-"`
	BodyPrealloc struct {
		// Position within the device
		DiskByteNr   int64 `bin:"off=0x0, siz=0x8"`
		DiskNumBytes int64 `bin:"off=0x8, siz=0x8"`

		// Position within the file
		Offset        int64 `bin:"off=0x10, siz=0x8"`
		NumBytes      int64 `bin:"off=0x18, siz=0x8"`
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
	case FILE_EXTENT_REG:
		_n, err := binstruct.Unmarshal(dat[n:], &o.BodyReg)
		n += _n
		if err != nil {
			return n, err
		}
	case FILE_EXTENT_PREALLOC:
		_n, err := binstruct.Unmarshal(dat[n:], &o.BodyPrealloc)
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
	case FILE_EXTENT_REG:
		bs, err := binstruct.Marshal(o.BodyReg)
		dat = append(dat, bs...)
		if err != nil {
			return dat, err
		}
	case FILE_EXTENT_PREALLOC:
		bs, err := binstruct.Marshal(o.BodyPrealloc)
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
