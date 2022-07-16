// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package scanforextents

import (
	"context"
	"errors"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

const csumBlockSize = 4 * 1024

func ChecksumLogical(fs btrfs.Trees, alg btrfssum.CSumType, laddr btrfsvol.LogicalAddr) (btrfssum.CSum, error) {
	var dat [csumBlockSize]byte
	if _, err := fs.ReadAt(dat[:], laddr); err != nil {
		return btrfssum.CSum{}, err
	}
	return alg.Sum(dat[:])
}

func ChecksumPhysical(dev *btrfs.Device, alg btrfssum.CSumType, paddr btrfsvol.PhysicalAddr) (btrfssum.CSum, error) {
	var dat [csumBlockSize]byte
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

type shortSum string

func readCSumTree(ctx context.Context, fs btrfs.Trees) map[shortSum][]btrfsvol.LogicalAddr {
	sb, _ := fs.Superblock()

	sum2laddrs := make(map[shortSum][]btrfsvol.LogicalAddr)
	var cntUnmapped, cntErr, cntMismatch, cntValid int
	fs.TreeWalk(ctx, btrfs.CSUM_TREE_OBJECTID,
		func(err *btrfs.TreeError) {
			dlog.Error(ctx, err)
		},
		btrfs.TreeWalkHandler{
			Item: func(path btrfs.TreePath, item btrfs.Item) error {
				if item.Key.ItemType != btrfsitem.EXTENT_CSUM_KEY {
					return nil
				}
				body := item.Body.(btrfsitem.ExtentCSum)

				for i, treeSum := range body.Sums {
					laddr := btrfsvol.LogicalAddr(item.Key.Offset) + (btrfsvol.LogicalAddr(i) * csumBlockSize)
					readSum, err := ChecksumLogical(fs, sb.ChecksumType, laddr)
					if err != nil {
						if errors.Is(err, btrfsvol.ErrCouldNotMap) {
							cntUnmapped++
							treeShortSum := shortSum(treeSum[:body.ChecksumSize])
							sum2laddrs[treeShortSum] = append(sum2laddrs[treeShortSum], laddr)
							continue
						}
						dlog.Error(ctx, err)
						cntErr++
						continue
					}
					if readSum != treeSum {
						dlog.Errorf(ctx, "checksum mismatch at laddr=%v: CSUM_TREE=%v != read=%v",
							laddr, treeSum, readSum)
						cntMismatch++
						continue
					}
					cntValid++
				}
				return nil
			},
		},
	)

	total := cntErr + cntUnmapped + cntMismatch + cntValid
	dlog.Infof(ctx, "  checksum errors          : %v", cntErr)
	dlog.Infof(ctx, "  unmapped checksums       : %v", cntUnmapped)
	dlog.Infof(ctx, "  mismatched checksums     : %v", cntMismatch)
	dlog.Infof(ctx, "  valid checksums          : %v", cntValid)
	dlog.Infof(ctx, "  -------------------------:")
	dlog.Infof(ctx, "  total checksums          : %v", total)
	dlog.Infof(ctx, "  distinct unmapped        : %v", len(sum2laddrs))

	return sum2laddrs
}

func LookupCSum(fs btrfs.Trees, alg btrfssum.CSumType, laddr btrfsvol.LogicalAddr) (map[btrfsvol.LogicalAddr]btrfssum.CSum, error) {
	item, err := fs.TreeSearch(btrfs.CSUM_TREE_OBJECTID, func(key btrfs.Key, size uint32) int {
		itemBeg := btrfsvol.LogicalAddr(key.ObjectID)
		numSums := int64(size) / int64(alg.Size())
		itemEnd := itemBeg + btrfsvol.LogicalAddr(numSums*csumBlockSize)
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
		ret[btrfsvol.LogicalAddr(item.Key.ObjectID)+(btrfsvol.LogicalAddr(i)*csumBlockSize)] = sum
	}
	return ret, nil
}
