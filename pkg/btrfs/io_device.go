package btrfs

import (
	"fmt"
	"os"
)

type Device struct {
	*os.File
}

func (dev Device) Size() (int64, error) {
	fi, err := dev.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

var superblockAddrs = []int64{
	0x00_0001_0000, // 64KiB
	0x00_0400_0000, // 64MiB
	0x40_0000_0000, // 256GiB
}

func (dev *Device) Superblocks() ([]Ref[Superblock], error) {
	const superblockSize = 0x1000

	sz, err := dev.Size()
	if err != nil {
		return nil, err
	}

	var ret []Ref[Superblock]
	for i, addr := range superblockAddrs {
		if addr+superblockSize <= sz {
			superblock := Ref[Superblock]{
				dev:  dev,
				addr: addr,
			}
			if err := superblock.Read(); err != nil {
				return nil, fmt.Errorf("superblock %d: %w", i, err)
			}
			ret = append(ret, superblock)
		}
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("no superblocks")
	}
	return ret, nil
}

func (dev *Device) superblock() (ret Ref[Superblock], err error) {
	sbs, err := dev.Superblocks()
	if err != nil {
		return ret, err
	}
	for i, sb := range sbs {
		if err := sb.Data.ValidateChecksum(); err != nil {
			return ret, fmt.Errorf("superblock %d: %w", i, err)
		}
		if i > 0 {
			if !sb.Data.Equal(sbs[0].Data) {
				return ret, fmt.Errorf("superblock %d and superblock %d disagree", 0, i)
			}
		}
	}
	return sbs[0], nil
}
