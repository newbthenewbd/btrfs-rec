// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

func ChecksumLogical(fs btrfs.Trees, alg btrfssum.CSumType, laddr btrfsvol.LogicalAddr) (btrfssum.CSum, error) {
	var dat [btrfsitem.CSumBlockSize]byte
	if _, err := fs.ReadAt(dat[:], laddr); err != nil {
		return btrfssum.CSum{}, err
	}
	return alg.Sum(dat[:])
}

func ChecksumPhysical(dev *btrfs.Device, alg btrfssum.CSumType, paddr btrfsvol.PhysicalAddr) (btrfssum.CSum, error) {
	var dat [btrfsitem.CSumBlockSize]byte
	if _, err := dev.ReadAt(dat[:], paddr); err != nil {
		return btrfssum.CSum{}, err
	}
	return alg.Sum(dat[:])
}

func ChecksumQualifiedPhysical(fs *btrfs.FS, alg btrfssum.CSumType, paddr btrfsvol.QualifiedPhysicalAddr) (btrfssum.CSum, error) {
	dev := fs.LV.PhysicalVolumes()[paddr.Dev]
	if dev == nil {
		return btrfssum.CSum{}, fmt.Errorf("no such device_id=%v", paddr.Dev)
	}
	return ChecksumPhysical(dev, alg, paddr.Addr)
}

func LookupCSum(fs btrfs.Trees, alg btrfssum.CSumType, laddr btrfsvol.LogicalAddr) (map[btrfsvol.LogicalAddr]btrfssum.CSum, error) {
	item, err := fs.TreeSearch(btrfs.CSUM_TREE_OBJECTID, func(key btrfs.Key, size uint32) int {
		itemBeg := btrfsvol.LogicalAddr(key.ObjectID)
		numSums := int64(size) / int64(alg.Size())
		itemEnd := itemBeg + btrfsvol.LogicalAddr(numSums*btrfsitem.CSumBlockSize)
		switch {
		case itemEnd <= laddr:
			return 1
		case laddr < itemBeg:
			return -1
		default:
			return 0
		}
	})
	if err != nil {
		return nil, err
	}
	body, ok := item.Body.(btrfsitem.ExtentCSum)
	if !ok {
		return nil, fmt.Errorf("item body is %T not ExtentCSum", item.Body)
	}
	ret := make(map[btrfsvol.LogicalAddr]btrfssum.CSum, len(body.Sums))
	for i, sum := range body.Sums {
		ret[btrfsvol.LogicalAddr(item.Key.ObjectID)+(btrfsvol.LogicalAddr(i)*btrfsitem.CSumBlockSize)] = sum
	}
	return ret, nil
}
