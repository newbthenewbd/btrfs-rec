// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"strings"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func MapLogicalSums(ctx context.Context, scanResults btrfsinspect.ScanDevicesResult) btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr] {
	dlog.Info(ctx, "Mapping the logical address space...")
	type record struct {
		Gen btrfs.Generation
		Sum btrfssum.ShortSum
	}
	addrspace := make(map[btrfsvol.LogicalAddr]record)
	var sumSize int
	for _, devResults := range scanResults {
		sumSize = devResults.Checksums.ChecksumSize
		for _, sumItem := range devResults.FoundExtentCSums {
			_ = sumItem.Sums.Walk(ctx, func(pos btrfsvol.LogicalAddr, sum btrfssum.ShortSum) error {
				new := record{
					Gen: sumItem.Generation,
					Sum: sum,
				}
				if old, ok := addrspace[pos]; ok {
					switch {
					case old.Gen > new.Gen:
						// do nothing
					case old.Gen < new.Gen:
						addrspace[pos] = new
					case old.Gen == new.Gen:
						if old != new {
							dlog.Errorf(ctx, "mismatch of laddr=%v sum: %v != %v", pos, old, new)
						}
					}
				} else {
					addrspace[pos] = new
				}
				return nil
			})
		}
	}
	dlog.Info(ctx, "... done mapping")

	var flattened btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr]
	if len(addrspace) == 0 {
		return flattened
	}

	dlog.Info(ctx, "Flattening the map ...")
	var curAddr btrfsvol.LogicalAddr
	var curSums strings.Builder
	for _, laddr := range maps.SortedKeys(addrspace) {
		if laddr != curAddr+(btrfsvol.LogicalAddr(curSums.Len()/sumSize)*btrfssum.BlockSize) {
			if curSums.Len() > 0 {
				flattened.Runs = append(flattened.Runs, btrfssum.SumRun[btrfsvol.LogicalAddr]{
					ChecksumSize: sumSize,
					Addr:         curAddr,
					Sums:         btrfssum.ShortSum(curSums.String()),
				})
			}
			curAddr = laddr
			curSums.Reset()
		}
		curSums.WriteString(string(addrspace[laddr].Sum))
	}
	if curSums.Len() > 0 {
		flattened.Runs = append(flattened.Runs, btrfssum.SumRun[btrfsvol.LogicalAddr]{
			ChecksumSize: sumSize,
			Addr:         curAddr,
			Sums:         btrfssum.ShortSum(curSums.String()),
		})
	}
	flattened.Addr = flattened.Runs[0].Addr
	last := flattened.Runs[len(flattened.Runs)-1]
	end := last.Addr.Add(last.Size())
	flattened.Size = end.Sub(flattened.Addr)
	dlog.Info(ctx, "... done flattening")

	return flattened
}

func SumsForLogicalRegion(sums btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr], beg btrfsvol.LogicalAddr, size btrfsvol.AddrDelta) btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr] {
	runs := btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr]{
		Addr: beg,
		Size: size,
	}
	for laddr := beg; laddr < beg.Add(size); {
		run, next, ok := sums.RunForAddr(laddr)
		if !ok {
			laddr = next
			continue
		}
		off := int((laddr-run.Addr)/btrfssum.BlockSize) * run.ChecksumSize
		deltaAddr := slices.Min[btrfsvol.AddrDelta](
			size-laddr.Sub(beg),
			btrfsvol.AddrDelta((len(run.Sums)-off)/run.ChecksumSize)*btrfssum.BlockSize)
		deltaOff := int(deltaAddr/btrfssum.BlockSize) * run.ChecksumSize
		runs.Runs = append(runs.Runs, btrfssum.SumRun[btrfsvol.LogicalAddr]{
			ChecksumSize: run.ChecksumSize,
			Addr:         laddr,
			Sums:         run.Sums[off : off+deltaOff],
		})
		laddr = laddr.Add(deltaAddr)
	}
	return runs
}
