package btrfsitem

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
)

type DirList []Dir // DIR_ITEM=84, DIR_INDEX=96, XATTR_ITEM=24

func (o *DirList) UnmarshalBinary(dat []byte) (int, error) {
	*o = nil
	n := 0
	for n < len(dat) {
		var ref Dir
		_n, err := binstruct.Unmarshal(dat, &ref)
		n += _n
		if err != nil {
			return n, err
		}
		*o = append(*o, ref)
	}
	return n, nil
}

func (o DirList) MarshalBinary() ([]byte, error) {
	var ret []byte
	for _, ref := range o {
		bs, err := binstruct.Marshal(ref)
		ret = append(ret, bs...)
		if err != nil {
			return ret, err
		}
	}
	return ret, nil
}

type Dir struct {
	Location      internal.Key `bin:"off=0x0, siz=0x11"`
	TransID       int64        `bin:"off=0x11, siz=8"`
	DataLen       uint16       `bin:"off=0x19, siz=2"`
	NameLen       uint16       `bin:"off=0x1b, siz=2"`
	Type          FileType     `bin:"off=0x1d, siz=1"`
	binstruct.End `bin:"off=0x1e"`
	Data          []byte `bin:"-"`
	Name          []byte `bin:"-"`
}

func (o *Dir) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.UnmarshalWithoutInterface(dat, o)
	if err != nil {
		return n, err
	}
	o.Data = dat[n : n+int(o.DataLen)]
	n += int(o.DataLen)
	o.Name = dat[n : n+int(o.NameLen)]
	n += int(o.NameLen)
	return n, nil
}

func (o Dir) MarshalBinary() ([]byte, error) {
	o.DataLen = uint16(len(o.Data))
	o.NameLen = uint16(len(o.Name))
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	dat = append(dat, o.Data...)
	dat = append(dat, o.Name...)
	return dat, nil
}

type FileType uint8

const (
	FT_UNKNOWN  = FileType(0)
	FT_REG_FILE = FileType(1)
	FT_DIR      = FileType(2)
	FT_CHRDEV   = FileType(3)
	FT_BLKDEV   = FileType(4)
	FT_FIFO     = FileType(5)
	FT_SOCK     = FileType(6)
	FT_SYMLINK  = FileType(7)
	FT_XATTR    = FileType(8)
	FT_MAX      = FileType(9)
)

func (ft FileType) String() string {
	names := map[FileType]string{
		FT_UNKNOWN:  "UNKNOWN",
		FT_REG_FILE: "FILE", // XXX
		FT_DIR:      "DIR",
		FT_CHRDEV:   "CHRDEV",
		FT_BLKDEV:   "BLKDEV",
		FT_FIFO:     "FIFO",
		FT_SOCK:     "SOCK",
		FT_SYMLINK:  "SYMLINK",
		FT_XATTR:    "XATTR",
	}
	if name, ok := names[ft]; ok {
		return name
	}
	return fmt.Sprintf("DIR_ITEM.%d", uint8(ft))
}
