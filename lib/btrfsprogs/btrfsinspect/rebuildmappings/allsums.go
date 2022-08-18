// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"encoding/gob"
	"io"
	"math"
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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
)

type AllSums struct {
	Logical  []btrfsinspect.SumRun[btrfsvol.LogicalAddr]
	Physical map[btrfsvol.DeviceID]btrfsinspect.SumRun[btrfsvol.PhysicalAddr]
}

func (as AllSums) SumForPAddr(paddr btrfsvol.QualifiedPhysicalAddr) (btrfsinspect.ShortSum, bool) {
	run, ok := as.Physical[paddr.Dev]
	if !ok {
		return "", false
	}
	return run.SumForAddr(paddr.Addr)
}

func (as AllSums) RunForLAddr(laddr btrfsvol.LogicalAddr) (btrfsinspect.SumRun[btrfsvol.LogicalAddr], btrfsvol.LogicalAddr, bool) {
	for _, run := range as.Logical {
		if run.Addr > laddr {
			return btrfsinspect.SumRun[btrfsvol.LogicalAddr]{}, run.Addr, false
		}
		if run.Addr.Add(run.Size()) <= laddr {
			continue
		}
		return run, 0, true
	}
	return btrfsinspect.SumRun[btrfsvol.LogicalAddr]{}, math.MaxInt64, false
}

func (as AllSums) SumForLAddr(laddr btrfsvol.LogicalAddr) (btrfsinspect.ShortSum, bool) {
	run, _, ok := as.RunForLAddr(laddr)
	if !ok {
		return "", false
	}
	return run.SumForAddr(laddr)
}

func (as AllSums) WalkLogical(ctx context.Context, fn func(btrfsvol.LogicalAddr, btrfsinspect.ShortSum) error) error {
	for _, run := range as.Logical {
		if err := run.Walk(ctx, fn); err != nil {
			return err
		}
	}
	return nil
}

// Read/Write AllSums ////////////////////////////////////////////////

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

func SumEverything(ctx context.Context, fs *btrfs.FS) (AllSums, error) {
	var ret AllSums

	// ChecksumSize
	var alg btrfssum.CSumType
	var csumSize int
	if err := func() error {
		sb, err := fs.Superblock()
		if err != nil {
			return err
		}
		alg = sb.ChecksumType
		csumSize = alg.Size()
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
						laddr := btrfsvol.LogicalAddr(item.Key.Offset) + (btrfsvol.LogicalAddr(i) * btrfsitem.CSumBlockSize)
						if laddr != curAddr+(btrfsvol.LogicalAddr(curSums.Len()/csumSize)*btrfsitem.CSumBlockSize) {
							if curSums.Len() > 0 {
								ret.Logical = append(ret.Logical, btrfsinspect.SumRun[btrfsvol.LogicalAddr]{
									ChecksumSize: csumSize,
									Addr:         curAddr,
									Sums:         curSums.String(),
								})
							}
							curAddr = laddr
							curSums.Reset()
						}
						curSums.Write(sum[:csumSize])
					}
					return nil
				},
			},
		)
		if curSums.Len() > 0 {
			ret.Logical = append(ret.Logical, btrfsinspect.SumRun[btrfsvol.LogicalAddr]{
				ChecksumSize: csumSize,
				Addr:         curAddr,
				Sums:         curSums.String(),
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
		ret.Physical = make(map[btrfsvol.DeviceID]btrfsinspect.SumRun[btrfsvol.PhysicalAddr], len(devs))

		grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
		for devID, dev := range devs {
			devID, dev := devID, dev
			grp.Go(dev.Name(), func(ctx context.Context) error {
				devSize := dev.Size()
				numSums := int(devSize / btrfsitem.CSumBlockSize)
				sums := make([]byte, numSums*csumSize)
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
					sum, err := btrfsutil.ChecksumPhysical(dev, alg, btrfsvol.PhysicalAddr(i*btrfsitem.CSumBlockSize))
					if err != nil {
						return err
					}
					copy(sums[i*csumSize:], sum[:csumSize])
				}
				progress(numSums)
				sumsStr := string(sums)
				mu.Lock()
				ret.Physical[devID] = btrfsinspect.SumRun[btrfsvol.PhysicalAddr]{
					ChecksumSize: csumSize,
					Addr:         0,
					Sums:         sumsStr,
				}
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
