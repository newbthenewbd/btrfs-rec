package btrfs

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type Device struct {
	*os.File

	cacheSuperblocks []*util.Ref[PhysicalAddr, Superblock]
	cacheSuperblock  *util.Ref[PhysicalAddr, Superblock]
}

func (dev Device) Size() (PhysicalAddr, error) {
	fi, err := dev.Stat()
	if err != nil {
		return 0, err
	}
	return PhysicalAddr(fi.Size()), nil
}

var SuperblockAddrs = []PhysicalAddr{
	0x00_0001_0000, // 64KiB
	0x00_0400_0000, // 64MiB
	0x40_0000_0000, // 256GiB
}

func (dev *Device) ReadAt(dat []byte, paddr PhysicalAddr) (int, error) {
	return dev.File.ReadAt(dat, int64(paddr))
}

func (dev *Device) WriteAt(dat []byte, paddr PhysicalAddr) (int, error) {
	return dev.File.WriteAt(dat, int64(paddr))
}

func (dev *Device) Superblocks() ([]*util.Ref[PhysicalAddr, Superblock], error) {
	if dev.cacheSuperblocks != nil {
		return dev.cacheSuperblocks, nil
	}
	superblockSize := PhysicalAddr(binstruct.StaticSize(Superblock{}))

	sz, err := dev.Size()
	if err != nil {
		return nil, err
	}

	var ret []*util.Ref[PhysicalAddr, Superblock]
	for i, addr := range SuperblockAddrs {
		if addr+superblockSize <= sz {
			superblock := &util.Ref[PhysicalAddr, Superblock]{
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
	dev.cacheSuperblocks = ret
	return ret, nil
}

func (dev *Device) Superblock() (*util.Ref[PhysicalAddr, Superblock], error) {
	if dev.cacheSuperblock != nil {
		return dev.cacheSuperblock, nil
	}
	sbs, err := dev.Superblocks()
	if err != nil {
		return nil, err
	}

	for i, sb := range sbs {
		if err := sb.Data.ValidateChecksum(); err != nil {
			return nil, fmt.Errorf("superblock %d: %w", i, err)
		}
		if i > 0 {
			if !sb.Data.Equal(sbs[0].Data) {
				return nil, fmt.Errorf("superblock %d and superblock %d disagree", 0, i)
			}
		}
	}

	dev.cacheSuperblock = sbs[0]
	return sbs[0], nil
}
