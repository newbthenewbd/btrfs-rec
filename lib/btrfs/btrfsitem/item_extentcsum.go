// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsitem

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

// An ExtentCSum checksums regions of the logical address space.
//
// Key:
//
//	key.objectid = BTRFS_EXTENT_CSUM_OBJECTID
//	key.offset   = laddr of checksummed region
type ExtentCSum struct { // trivial EXTENT_CSUM=128
	// Checksum of each sector starting at key.offset
	btrfssum.SumRun[btrfsvol.LogicalAddr]
}

func (o *ExtentCSum) UnmarshalBinary(dat []byte) (int, error) {
	if o.ChecksumSize == 0 {
		return 0, fmt.Errorf(".ChecksumSize must be set")
	}
	o.Sums = btrfssum.ShortSum(dat)
	return len(dat), nil
}

func (o ExtentCSum) MarshalBinary() ([]byte, error) {
	if o.ChecksumSize == 0 {
		return nil, fmt.Errorf(".ChecksumSize must be set")
	}
	return []byte(o.Sums), nil
}
