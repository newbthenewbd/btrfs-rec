// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

// dat doesn't escape to the heap in .ReadAt(dat, …), but the compiler
// can't figure that out, so we use a Pool for our byte arrays, since
// the compiler won't let us allocate them on the stack.
var blockPool containers.SlicePool[byte]

func ChecksumLogical(fs diskio.File[btrfsvol.LogicalAddr], alg btrfssum.CSumType, laddr btrfsvol.LogicalAddr) (btrfssum.CSum, error) {
	dat := blockPool.Get(btrfssum.BlockSize)
	defer blockPool.Put(dat)
	if _, err := fs.ReadAt(dat, laddr); err != nil {
		return btrfssum.CSum{}, err
	}
	return alg.Sum(dat)
}

func ChecksumPhysical(dev *Device, alg btrfssum.CSumType, paddr btrfsvol.PhysicalAddr) (btrfssum.CSum, error) {
	dat := blockPool.Get(btrfssum.BlockSize)
	defer blockPool.Put(dat)
	if _, err := dev.ReadAt(dat, paddr); err != nil {
		return btrfssum.CSum{}, err
	}
	return alg.Sum(dat)
}

func ChecksumQualifiedPhysical(fs *FS, alg btrfssum.CSumType, paddr btrfsvol.QualifiedPhysicalAddr) (btrfssum.CSum, error) {
	dev := fs.LV.PhysicalVolumes()[paddr.Dev]
	if dev == nil {
		return btrfssum.CSum{}, fmt.Errorf("no such device_id=%v", paddr.Dev)
	}
	return ChecksumPhysical(dev, alg, paddr.Addr)
}

func LookupCSum(fs btrfstree.TreeOperator, alg btrfssum.CSumType, laddr btrfsvol.LogicalAddr) (btrfssum.SumRun[btrfsvol.LogicalAddr], error) {
	item, err := fs.TreeSearch(btrfsprim.CSUM_TREE_OBJECTID, btrfstree.SearchCSum(laddr, alg.Size()))
	if err != nil {
		return btrfssum.SumRun[btrfsvol.LogicalAddr]{}, err
	}
	if item.Key.ItemType != btrfsitem.EXTENT_CSUM_KEY {
		return btrfssum.SumRun[btrfsvol.LogicalAddr]{}, fmt.Errorf("item type is %v, not EXTENT_CSUM", item.Key.ItemType)
	}
	switch body := item.Body.(type) {
	case *btrfsitem.ExtentCSum:
		return body.SumRun, nil
	case *btrfsitem.Error:
		return btrfssum.SumRun[btrfsvol.LogicalAddr]{}, body.Err
	default:
		panic(fmt.Errorf("should not happen: EXTENT_CSUM has unexpected item type: %T", body))
	}
}
