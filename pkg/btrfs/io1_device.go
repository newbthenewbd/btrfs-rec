package btrfs

import (
	"fmt"
	"os"

	. "lukeshu.com/btrfs-tools/pkg/btrfs/btrfstyp"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type Device struct {
	*os.File
}

func (dev Device) Size() (PhysicalAddr, error) {
	fi, err := dev.Stat()
	if err != nil {
		return 0, err
	}
	return PhysicalAddr(fi.Size()), nil
}

var superblockAddrs = []PhysicalAddr{
	0x00_0001_0000, // 64KiB
	0x00_0400_0000, // 64MiB
	0x40_0000_0000, // 256GiB
}

func (dev *Device) ReadAt(dat []byte, paddr PhysicalAddr) (int, error) {
	return dev.File.ReadAt(dat, int64(paddr))
}

func (dev *Device) Superblocks() ([]util.Ref[PhysicalAddr, Superblock], error) {
	const superblockSize = 0x1000

	sz, err := dev.Size()
	if err != nil {
		return nil, err
	}

	var ret []util.Ref[PhysicalAddr, Superblock]
	for i, addr := range superblockAddrs {
		if addr+superblockSize <= sz {
			superblock := util.Ref[PhysicalAddr, Superblock]{
				File: dev,
				Addr: addr,
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

func (dev *Device) Superblock() (ret util.Ref[PhysicalAddr, Superblock], err error) {
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
