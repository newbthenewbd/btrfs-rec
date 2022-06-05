package btrfsitem

import (
	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type InodeRefList []InodeRef // INODE_REF=12

func (o *InodeRefList) UnmarshalBinary(dat []byte) (int, error) {
	*o = nil
	n := 0
	for n < len(dat) {
		var ref InodeRef
		_n, err := binstruct.Unmarshal(dat, &ref)
		n += _n
		if err != nil {
			return n, err
		}
		*o = append(*o, ref)
	}
	return n, nil
}

func (o InodeRefList) MarshalBinary() ([]byte, error) {
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

type InodeRef struct {
	Index         int64  `bin:"off=0x0, siz=0x8"`
	NameLen       uint16 `bin:"off=0x8, siz=0x2"`
	binstruct.End `bin:"off=0xa"`
	Name          []byte `bin:"-"`
}

func (o *InodeRef) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.UnmarshalWithoutInterface(dat, o)
	if err != nil {
		return n, err
	}
	dat = dat[n:]
	o.Name = dat[:o.NameLen]
	n += int(o.NameLen)
	return n, nil
}

func (o InodeRef) MarshalBinary() ([]byte, error) {
	o.NameLen = uint16(len(o.Name))
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	dat = append(dat, o.Name...)
	return dat, nil
}
