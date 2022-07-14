// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"fmt"
	"os"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type Device struct {
	*os.File

	cacheSuperblocks []*diskio.Ref[btrfsvol.PhysicalAddr, Superblock]
	cacheSuperblock  *Superblock
}

var _ diskio.File[btrfsvol.PhysicalAddr] = (*Device)(nil)

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

func (dev *Device) Superblocks() ([]*diskio.Ref[btrfsvol.PhysicalAddr, Superblock], error) {
	if dev.cacheSuperblocks != nil {
		return dev.cacheSuperblocks, nil
	}
	superblockSize := btrfsvol.PhysicalAddr(binstruct.StaticSize(Superblock{}))

	sz, err := dev.Size()
	if err != nil {
		return nil, err
	}

	var ret []*diskio.Ref[btrfsvol.PhysicalAddr, Superblock]
	for i, addr := range SuperblockAddrs {
		if addr+superblockSize <= sz {
			superblock := &diskio.Ref[btrfsvol.PhysicalAddr, Superblock]{
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

func (dev *Device) Superblock() (*Superblock, error) {
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

	dev.cacheSuperblock = &sbs[0].Data
	return &sbs[0].Data, nil
}
