// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"encoding/hex"
	"fmt"
	"io"

	"git.lukeshu.com/go/lowmemjson"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
)

const CSumBlockSize = 4 * 1024

// key.objectid = BTRFS_EXTENT_CSUM_OBJECTID
// key.offset = laddr of checksummed region
type ExtentCSum struct { // EXTENT_CSUM=128
	ChecksumSize int
	// Checksum of each sector starting at key.offset
	Sums []btrfssum.CSum
}

func (o *ExtentCSum) UnmarshalBinary(dat []byte) (int, error) {
	if o.ChecksumSize == 0 {
		return 0, fmt.Errorf(".ChecksumSize must be set")
	}
	for len(dat) >= o.ChecksumSize {
		var csum btrfssum.CSum
		copy(csum[:], dat[:o.ChecksumSize])
		dat = dat[o.ChecksumSize:]
		o.Sums = append(o.Sums, csum)
	}
	return len(o.Sums) * o.ChecksumSize, nil
}

func (o ExtentCSum) MarshalBinary() ([]byte, error) {
	if o.ChecksumSize == 0 {
		return nil, fmt.Errorf(".ChecksumSize must be set")
	}
	var dat []byte
	for _, csum := range o.Sums {
		dat = append(dat, csum[:o.ChecksumSize]...)
	}
	return dat, nil
}

var (
	_ lowmemjson.Encodable = ExtentCSum{}
)

func (o ExtentCSum) EncodeJSON(w io.Writer) error {
	if _, err := fmt.Fprintf(w, `{"ChecksumSize":%d,"Sums":[`, o.ChecksumSize); err != nil {
		return err
	}
	for i, sum := range o.Sums {
		if i > 0 {
			if _, err := w.Write([]byte(",")); err != nil {
				return err
			}
		}
		if _, err := w.Write([]byte(`"`)); err != nil {
			return err
		}
		if _, err := hex.NewEncoder(w).Write(sum[:o.ChecksumSize]); err != nil {
			return err
		}
		if _, err := w.Write([]byte(`"`)); err != nil {
			return err
		}
	}
	if _, err := w.Write([]byte(`]}`)); err != nil {
		return err
	}
	return nil
}
