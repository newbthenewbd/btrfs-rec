// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package scanforextents

import (
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
	"sync"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
)

const csumBlockSize = 4 * 1024

type ShortSum string

type LogicalSumRun struct {
	Addr btrfsvol.LogicalAddr
	Sums string
}

type AllSums struct {
	ChecksumSize int
	Logical      []LogicalSumRun
	Physical     map[btrfsvol.DeviceID]string
}

func ReadAllSums(filename string) (AllSums, error) {
	fh, err := os.Open(filename)
	if err != nil {
		return AllSums{}, err
	}
	defer fh.Close()
	var val AllSums
	if err := gob.NewDecoder(fh).Decode(&val); err != nil {
		return AllSums{}, err
	}
	return val, nil
}

func WriteAllSums(w io.Writer, sums AllSums) error {
	return gob.NewEncoder(w).Encode(sums)
}

func (as AllSums) SumForPAddr(paddr btrfsvol.QualifiedPhysicalAddr) (ShortSum, bool) {
	run, ok := as.Physical[paddr.Dev]
	if !ok {
		return "", false
	}
	off := int(paddr.Addr/csumBlockSize) * as.ChecksumSize
	if off+as.ChecksumSize > len(run) {
		return "", false
	}
	return ShortSum(run[off : off+as.ChecksumSize]), true
}

func (as AllSums) SumForLAddr(laddr btrfsvol.LogicalAddr) (ShortSum, bool) {
	for _, run := range as.Logical {
		size := btrfsvol.AddrDelta(len(run.Sums)/as.ChecksumSize) * csumBlockSize
		if run.Addr > laddr {
			return "", false
		}
		if run.Addr.Add(size) <= laddr {
			continue
		}
		off := int(laddr.Sub(run.Addr)/csumBlockSize) * as.ChecksumSize
		return ShortSum(run.Sums[off : off+as.ChecksumSize]), true
	}
	return "", false
}

func (as AllSums) WalkLogical(fn func(btrfsvol.LogicalAddr, ShortSum) error) error {
	for _, run := range as.Logical {
		for laddr, off := run.Addr, 0; off < len(run.Sums); laddr, off = laddr+csumBlockSize, off+as.ChecksumSize {
			if err := fn(laddr, ShortSum(run.Sums[off:off+as.ChecksumSize])); err != nil {
				return err
			}
		}
	}
	return nil
}

func SumEverything(ctx context.Context, fs *btrfs.FS) (AllSums, error) {
	var ret AllSums

	// ChecksumSize
	var alg btrfssum.CSumType
	if err := func() error {
		sb, err := fs.Superblock()
		if err != nil {
			return err
		}
		alg = sb.ChecksumType
		ret.ChecksumSize = alg.Size()
		return nil
	}(); err != nil {
		return ret, err
	}

	// Logical
	dlog.Info(ctx, "Walking CSUM_TREE...")
	func() {
		var curAddr btrfsvol.LogicalAddr
		var curSums strings.Builder
		btrfsutil.NewBrokenTrees(ctx, fs).TreeWalk(ctx, btrfs.CSUM_TREE_OBJECTID,
			func(err *btrfs.TreeError) {
				dlog.Error(ctx, err)
			},
			btrfs.TreeWalkHandler{
				Item: func(path btrfs.TreePath, item btrfs.Item) error {
					if item.Key.ItemType != btrfsitem.EXTENT_CSUM_KEY {
						return nil
					}
					body := item.Body.(btrfsitem.ExtentCSum)

					for i, sum := range body.Sums {
						laddr := btrfsvol.LogicalAddr(item.Key.Offset) + (btrfsvol.LogicalAddr(i) * csumBlockSize)
						if laddr != curAddr {
							if curSums.Len() > 0 {
								ret.Logical = append(ret.Logical, LogicalSumRun{
									Addr: curAddr,
									Sums: curSums.String(),
								})
							}
							curAddr = laddr
							curSums.Reset()
						}
						curSums.Write(sum[:ret.ChecksumSize])
					}
					return nil
				},
			},
		)
		if curSums.Len() > 0 {
			ret.Logical = append(ret.Logical, LogicalSumRun{
				Addr: curAddr,
				Sums: curSums.String(),
			})
		}
	}()
	if err := ctx.Err(); err != nil {
		return ret, err
	}
	dlog.Info(ctx, "... done walking")
	runtime.GC()
	dlog.Info(ctx, "... GC'd")

	// Physical
	dlog.Info(ctx, "Summing devices...")
	if err := func() error {
		devs := fs.LV.PhysicalVolumes()

		var mu sync.Mutex
		ret.Physical = make(map[btrfsvol.DeviceID]string, len(devs))

		grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
		for devID, dev := range devs {
			devID, dev := devID, dev
			grp.Go(dev.Name(), func(ctx context.Context) error {
				devSize := dev.Size()
				numSums := int(devSize / csumBlockSize)
				sums := make([]byte, numSums*ret.ChecksumSize)
				lastPct := -1
				progress := func(curSum int) {
					pct := int(100 * float64(curSum) / float64(numSums))
					if pct != lastPct || curSum == numSums {
						dlog.Infof(ctx, "... dev[%q] summed %v%%",
							dev.Name(), pct)
						lastPct = pct
					}
				}
				for i := 0; i < numSums; i++ {
					if err := ctx.Err(); err != nil {
						return err
					}
					progress(i)
					sum, err := ChecksumPhysical(dev, alg, btrfsvol.PhysicalAddr(i*csumBlockSize))
					if err != nil {
						return err
					}
					copy(sums[i*ret.ChecksumSize:], sum[:ret.ChecksumSize])
				}
				progress(numSums)
				sumsStr := string(sums)
				mu.Lock()
				ret.Physical[devID] = sumsStr
				mu.Unlock()
				return nil
			})
		}
		return grp.Wait()
	}(); err != nil {
		return ret, err
	}
	dlog.Info(ctx, "... done summing devices")
	runtime.GC()
	dlog.Info(ctx, "... GC'd")

	// Return
	return ret, nil
}

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
