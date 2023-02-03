// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
)

// Metadata is like Extent, but doesn't have .Info.
type Metadata struct { // complex METADATA_ITEM=169
	Head ExtentHeader
	Refs []ExtentInlineRef
}

func (o *Metadata) Free() {
	for i := range o.Refs {
		if o.Refs[i].Body != nil {
			o.Refs[i].Body.Free()
		}
		o.Refs[i] = ExtentInlineRef{}
	}
	extentInlineRefPool.Put(o.Refs)
	*o = Metadata{}
	metadataPool.Put(o)
}

func (o Metadata) Clone() Metadata {
	ret := o
	ret.Refs = extentInlineRefPool.Get(len(o.Refs))
	copy(ret.Refs, o.Refs)
	for i := range ret.Refs {
		ret.Refs[i].Body = o.Refs[i].Body.CloneItem()
	}
	return ret
}

func (o *Metadata) UnmarshalBinary(dat []byte) (int, error) {
	*o = Metadata{}
	n, err := binstruct.Unmarshal(dat, &o.Head)
	if err != nil {
		return n, err
	}
	if n < len(dat) {
		o.Refs = extentInlineRefPool.Get(1)[:0]
	}
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

func (o Metadata) MarshalBinary() ([]byte, error) {
	dat, err := binstruct.Marshal(o.Head)
	if err != nil {
		return dat, err
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
