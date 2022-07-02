package btrfs

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type Device struct {
	*os.File

	cacheSuperblocks []*util.Ref[btrfsvol.PhysicalAddr, Superblock]
	cacheSuperblock  *util.Ref[btrfsvol.PhysicalAddr, Superblock]
}

var _ util.File[btrfsvol.PhysicalAddr] = (*Device)(nil)

func (dev Device) Size() (btrfsvol.PhysicalAddr, error) {
	fi, err := dev.Stat()
	if err != nil {
		return 0, err
	}
	return btrfsvol.PhysicalAddr(fi.Size()), nil
}

func (dev *Device) ReadAt(dat []byte, paddr btrfsvol.PhysicalAddr) (int, error) {
	return dev.File.ReadAt(dat, int64(paddr))
}

func (dev *Device) WriteAt(dat []byte, paddr btrfsvol.PhysicalAddr) (int, error) {
	return dev.File.WriteAt(dat, int64(paddr))
}

var SuperblockAddrs = []btrfsvol.PhysicalAddr{
	0x00_0001_0000, // 64KiB
	0x00_0400_0000, // 64MiB
	0x40_0000_0000, // 256GiB
}

func (dev *Device) Superblocks() ([]*util.Ref[btrfsvol.PhysicalAddr, Superblock], error) {
	if dev.cacheSuperblocks != nil {
		return dev.cacheSuperblocks, nil
	}
	superblockSize := btrfsvol.PhysicalAddr(binstruct.StaticSize(Superblock{}))

	sz, err := dev.Size()
	if err != nil {
		return nil, err
	}

	var ret []*util.Ref[btrfsvol.PhysicalAddr, Superblock]
	for i, addr := range SuperblockAddrs {
		if addr+superblockSize <= sz {
			superblock := &util.Ref[btrfsvol.PhysicalAddr, Superblock]{
				File: dev,
				Addr: addr,
			}
			if err := superblock.Read(); err != nil {
				return nil, fmt.Errorf("superblock %v: %w", i, err)
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

func (dev *Device) Superblock() (*util.Ref[btrfsvol.PhysicalAddr, Superblock], error) {
	if dev.cacheSuperblock != nil {
		return dev.cacheSuperblock, nil
	}
	sbs, err := dev.Superblocks()
	if err != nil {
		return nil, err
	}

	for i, sb := range sbs {
		if err := sb.Data.ValidateChecksum(); err != nil {
			return nil, fmt.Errorf("superblock %v: %w", i, err)
		}
		if i > 0 {
			if !sb.Data.Equal(sbs[0].Data) {
				return nil, fmt.Errorf("superblock %v and superblock %v disagree", 0, i)
			}
		}
	}

	dev.cacheSuperblock = sbs[0]
	return sbs[0], nil
}
