// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
)

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
