// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
)

// key.objectid = laddr of the extent
// key.offset = length of the extent
type Extent struct { // EXTENT_ITEM=168
	Head ExtentHeader
	Info TreeBlockInfo // only if .Head.Flags.Has(EXTENT_FLAG_TREE_BLOCK)
	Refs []ExtentInlineRef
}

func (o *Extent) UnmarshalBinary(dat []byte) (int, error) {
	n, err := binstruct.Unmarshal(dat, &o.Head)
	if err != nil {
		return n, err
	}
	if o.Head.Flags.Has(EXTENT_FLAG_TREE_BLOCK) {
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
	if o.Head.Flags.Has(EXTENT_FLAG_TREE_BLOCK) {
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
	Refs          int64                `bin:"off=0, siz=8"`
	Generation    btrfsprim.Generation `bin:"off=8, siz=8"`
	Flags         ExtentFlags          `bin:"off=16, siz=8"`
	binstruct.End `bin:"off=24"`
}

type TreeBlockInfo struct {
	Key           btrfsprim.Key `bin:"off=0, siz=0x11"`
	Level         uint8         `bin:"off=0x11, siz=0x1"`
	binstruct.End `bin:"off=0x12"`
}

type ExtentFlags uint64

const (
	EXTENT_FLAG_DATA = ExtentFlags(1 << iota)
	EXTENT_FLAG_TREE_BLOCK
)

var extentFlagNames = []string{
	"DATA",
	"TREE_BLOCK",
}

func (f ExtentFlags) Has(req ExtentFlags) bool { return f&req == req }
func (f ExtentFlags) String() string {
	return fmtutil.BitfieldString(f, extentFlagNames, fmtutil.HexNone)
}

type ExtentInlineRef struct {
	Type   Type   // only 4 valid values: {TREE,SHARED}_BLOCK_REF_KEY, {EXTENT,SHARED}_DATA_REF_KEY
	Offset uint64 // only when Type != EXTENT_DATA_REF_KEY
	Body   Item   // only when Type == *_DATA_REF_KEY
}

func (o *ExtentInlineRef) UnmarshalBinary(dat []byte) (int, error) {
	o.Type = Type(dat[0])
	n := 1
	switch o.Type {
	case TREE_BLOCK_REF_KEY, SHARED_BLOCK_REF_KEY:
		_n, err := binstruct.Unmarshal(dat[n:], &o.Offset)
		n += _n
		if err != nil {
			return n, err
		}
	case EXTENT_DATA_REF_KEY:
		var dref ExtentDataRef
		_n, err := binstruct.Unmarshal(dat[n:], &dref)
		n += _n
		o.Body = dref
		if err != nil {
			return n, err
		}
	case SHARED_DATA_REF_KEY:
		_n, err := binstruct.Unmarshal(dat[n:], &o.Offset)
		n += _n
		if err != nil {
			return n, err
		}
		var sref SharedDataRef
		_n, err = binstruct.Unmarshal(dat[n:], &sref)
		n += _n
		o.Body = sref
		if err != nil {
			return n, err
		}
	default:
		return n, fmt.Errorf("unexpected item type %v", o.Type)
	}
	return n, nil
}

func (o ExtentInlineRef) MarshalBinary() ([]byte, error) {
	dat := []byte{byte(o.Type)}
	switch o.Type {
	case TREE_BLOCK_REF_KEY, SHARED_BLOCK_REF_KEY:
		_dat, err := binstruct.Marshal(o.Offset)
		dat = append(dat, _dat...)
		if err != nil {
			return dat, err
		}
	case EXTENT_DATA_REF_KEY:
		_dat, err := binstruct.Marshal(o.Body)
		dat = append(dat, _dat...)
		if err != nil {
			return dat, err
		}
	case SHARED_DATA_REF_KEY:
		_dat, err := binstruct.Marshal(o.Offset)
		dat = append(dat, _dat...)
		if err != nil {
			return dat, err
		}
		_dat, err = binstruct.Marshal(o.Body)
		dat = append(dat, _dat...)
		if err != nil {
			return dat, err
		}
	default:
		return dat, fmt.Errorf("unexpected item type %v", o.Type)
	}
	return dat, nil
}
