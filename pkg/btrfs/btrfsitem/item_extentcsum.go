package btrfsitem

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfssum"
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
		return 0, fmt.Errorf("btrfs.ExtentCSum.UnmarshalBinary: .ChecksumSize must be set")
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
		return nil, fmt.Errorf("btrfs.ExtentCSum.MarshalBinary: .ChecksumSize must be set")
	}
	var dat []byte
	for _, csum := range o.Sums {
		dat = append(dat, csum[:o.ChecksumSize]...)
	}
	return dat, nil
}
