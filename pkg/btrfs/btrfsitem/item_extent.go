package btrfsitem

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/internal"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type Extent struct { // EXTENT_ITEM=168
	Head ExtentHeader
	Info TreeBlockInfo // only if .Head.Flags.Has(BTRFS_EXTENT_FLAG_TREE_BLOCK)
	Refs []ExtentInlineRef
}

func (o *Extent) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.Unmarshal(dat, &o.Head)
	if err != nil {
		return n, err
	}
	if o.Head.Flags.Has(BTRFS_EXTENT_FLAG_TREE_BLOCK) {
		_n, err := binstruct.Unmarshal(dat[n:], &o.Info)
		n += _n
		if err != nil {
			return n, err
		}
	}
	o.Refs = nil
	for n < len(dat) {
		var ref ExtentInlineRef
		_n, err := binstruct.Unmarshal(dat[n:], &ref)
		n += _n
		o.Refs = append(o.Refs, ref)
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (o Extent) MarshalBinary() ([]byte, error) {
	dat, err := binstruct.Marshal(o.Head)
	if err != nil {
		return dat, err
	}
	if o.Head.Flags.Has(BTRFS_EXTENT_FLAG_TREE_BLOCK) {
		bs, err := binstruct.Marshal(o.Info)
		dat = append(dat, bs...)
		if err != nil {
			return dat, err
		}
	}
	for _, ref := range o.Refs {
		bs, err := binstruct.Marshal(ref)
		dat = append(dat, bs...)
		if err != nil {
			return dat, err
		}
	}
	return dat, nil
}

type ExtentHeader struct {
	Refs          int64       `bin:"off=0, siz=8"`
	Generation    int64       `bin:"off=8, siz=8"`
	Flags         ExtentFlags `bin:"off=16, siz=8"`
	binstruct.End `bin:"off=24"`
}

type TreeBlockInfo struct {
	Key           internal.Key `bin:"off=0, siz=0x11"`
	Level         uint8        `bin:"off=0x11, siz=0x8"`
	binstruct.End `bin:"off=0x19"`
}

type ExtentFlags uint64

const (
	BTRFS_EXTENT_FLAG_DATA = ExtentFlags(1 << iota)
	BTRFS_EXTENT_FLAG_TREE_BLOCK
)

var extentFlagNames = []string{
	"DATA",
	"TREE_BLOCK",
}

func (f ExtentFlags) Has(req ExtentFlags) bool { return f&req == req }
func (f ExtentFlags) String() string           { return util.BitfieldString(f, extentFlagNames) }

type ExtentInlineRef struct {
	Type          Type   `bin:"off=0, siz=1"`
	Offset        uint64 `bin:"off=1, siz=8"`
	binstruct.End `bin:"off=9"`
	Body          Item `bin:"-"`
}

func (o *ExtentInlineRef) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.UnmarshalWithoutInterface(dat, o)
	if err != nil {
		return n, err
	}
	switch o.Type {
	case TREE_BLOCK_REF_KEY, SHARED_BLOCK_REF_KEY:
		o.Body = Empty{}
	case EXTENT_DATA_REF_KEY:
		return n, fmt.Errorf("the C code to do this doesn't make any sense")
	case SHARED_DATA_REF_KEY:
		var sref SharedDataRef
		_n, err := binstruct.Unmarshal(dat[n:], &sref)
		n += _n
		o.Body = sref
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

func (o ExtentInlineRef) MarshalBinary() ([]byte, error) {
	dat, err := binstruct.MarshalWithoutInterface(o)
	if err != nil {
		return dat, err
	}
	bs, err := binstruct.Marshal(o.Body)
	dat = append(dat, bs...)
	if err != nil {
		return dat, err
	}
	return dat, nil
}
