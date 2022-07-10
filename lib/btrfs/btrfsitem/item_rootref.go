package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/internal"
)

type RootRef struct { // ROOT_REF=156 ROOT_BACKREF=144
	DirID         internal.ObjID `bin:"off=0x00, siz=0x8"`
	Sequence      int64          `bin:"off=0x08, siz=0x8"`
	NameLen       uint16         `bin:"off=0x10, siz=0x2"` // [ignored-when-writing]
	binstruct.End `bin:"off=0x12"`
	Name          []byte `bin:"-"`
}

func (o *RootRef) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.UnmarshalWithoutInterface(dat, o)
	if err != nil {
		return n, err
	}
	o.Name = dat[n : n+int(o.NameLen)]
	n += int(o.NameLen)
	return n, nil
}

func (o RootRef) MarshalBinary() ([]byte, error) {
	o.NameLen = uint16(len(o.Name))
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	dat = append(dat, o.Name...)
	return dat, nil
}
