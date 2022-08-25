// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"sort"
	"strings"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func ExtractLogicalSums(ctx context.Context, scanResults btrfsinspect.ScanDevicesResult) btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr] {
	var records []btrfsinspect.SysExtentCSum
	for _, devResults := range scanResults {
		records = append(records, devResults.FoundExtentCSums...)
	}
	// Sort lower-generation items earlier; then sort by addr.
	sort.Slice(records, func(i, j int) bool {
		a, b := records[i], records[j]
		switch {
		case a.Generation < b.Generation:
			return true
		case a.Generation > b.Generation:
			return false
		default:
			return a.Sums.Addr < b.Sums.Addr
		}
	})
	if len(records) == 0 {
		return btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr]{}
	}
	sumSize := records[0].Sums.ChecksumSize

	// Now build them in to a coherent address space.
	//
	// We can't just reverse-sort by generation to avoid mutations, because given
	//
	//	gen1 AAAAAAA
	//      gen2    BBBBBBBB
	//      gen3          CCCCCCC
	//
	// "AAAAAAA" shouldn't be present, and if we just discard "BBBBBBBB"
	// because it conflicts with "CCCCCCC", then we would erroneously
	// include "AAAAAAA".
	addrspace := &containers.RBTree[containers.NativeOrdered[btrfsvol.LogicalAddr], btrfsinspect.SysExtentCSum]{
		KeyFn: func(item btrfsinspect.SysExtentCSum) containers.NativeOrdered[btrfsvol.LogicalAddr] {
			return containers.NativeOrdered[btrfsvol.LogicalAddr]{Val: item.Sums.Addr}
		},
	}
	for _, newRecord := range records {
		for {
			conflict := addrspace.Search(func(oldRecord btrfsinspect.SysExtentCSum) int {
				switch {
				case newRecord.Sums.Addr.Add(newRecord.Sums.Size()) <= oldRecord.Sums.Addr:
					// 'newRecord' is wholly to the left of 'oldRecord'.
					return -1
				case oldRecord.Sums.Addr.Add(oldRecord.Sums.Size()) <= newRecord.Sums.Addr:
					// 'oldRecord' is wholly to the left of 'newRecord'.
					return 1
				default:
					// There is some overlap.
					return 0
				}
			})
			if conflict == nil {
				// We can insert it
				addrspace.Insert(newRecord)
				break
			}
			oldRecord := conflict.Value
			if oldRecord == newRecord {
				// Duplicates are to be expected.
				break
			}
			if oldRecord.Generation < newRecord.Generation {
				// Newer generation wins.
				addrspace.Delete(containers.NativeOrdered[btrfsvol.LogicalAddr]{Val: oldRecord.Sums.Addr})
				// loop around to check for more conflicts
				continue
			}
			if oldRecord.Generation > newRecord.Generation {
				// We sorted them!  This shouldn't happen.
				panic("should not happen")
			}
			// Since sums are stored multiple times (RAID?), but may
			// be split between items differently between copies, we
			// should take the union (after verifying that they
			// agree on the overlapping part).
			overlapBeg := slices.Max(
				oldRecord.Sums.Addr,
				newRecord.Sums.Addr)
			overlapEnd := slices.Min(
				oldRecord.Sums.Addr.Add(oldRecord.Sums.Size()),
				newRecord.Sums.Addr.Add(newRecord.Sums.Size()))

			oldOverlapBeg := int(overlapBeg.Sub(oldRecord.Sums.Addr)/btrfssum.BlockSize) * sumSize
			oldOverlapEnd := int(overlapEnd.Sub(oldRecord.Sums.Addr)/btrfssum.BlockSize) * sumSize
			oldOverlap := oldRecord.Sums.Sums[oldOverlapBeg:oldOverlapEnd]

			newOverlapBeg := int(overlapBeg.Sub(newRecord.Sums.Addr)/btrfssum.BlockSize) * sumSize
			newOverlapEnd := int(overlapEnd.Sub(newRecord.Sums.Addr)/btrfssum.BlockSize) * sumSize
			newOverlap := newRecord.Sums.Sums[newOverlapBeg:newOverlapEnd]

			if oldOverlap != newOverlap {
				dlog.Errorf(ctx, "mismatch: {gen:%v, addr:%v, size:%v} conflicts with {gen:%v, addr:%v, size:%v}",
					oldRecord.Generation, oldRecord.Sums.Addr, oldRecord.Sums.Size(),
					newRecord.Generation, newRecord.Sums.Addr, newRecord.Sums.Size())
				break
			}
			// OK, we match, so take the union.
			var prefix, suffix btrfssum.ShortSum
			switch {
			case oldRecord.Sums.Addr < overlapBeg:
				prefix = oldRecord.Sums.Sums[:oldOverlapBeg]
			case newRecord.Sums.Addr < overlapBeg:
				prefix = newRecord.Sums.Sums[:newOverlapBeg]
			}
			switch {
			case oldRecord.Sums.Addr.Add(oldRecord.Sums.Size()) > overlapEnd:
				suffix = oldRecord.Sums.Sums[oldOverlapEnd:]
			case newRecord.Sums.Addr.Add(newRecord.Sums.Size()) > overlapEnd:
				suffix = newRecord.Sums.Sums[newOverlapEnd:]
			}
			unionRecord := btrfsinspect.SysExtentCSum{
				Generation: oldRecord.Generation,
				Sums: btrfsitem.ExtentCSum{
					SumRun: btrfssum.SumRun[btrfsvol.LogicalAddr]{
						ChecksumSize: oldRecord.Sums.ChecksumSize,
						Addr:         slices.Min(oldRecord.Sums.Addr, newRecord.Sums.Addr),
						Sums:         prefix + oldOverlap + suffix,
					},
				},
			}
			addrspace.Delete(containers.NativeOrdered[btrfsvol.LogicalAddr]{Val: oldRecord.Sums.Addr})
			newRecord = unionRecord
			// loop around to check for more conflicts
		}
	}

	// Now flatten that RBTree in to a SumRunWithGaps.
	var flattened btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr]
	var curAddr btrfsvol.LogicalAddr
	var curSums strings.Builder
	_ = addrspace.Walk(func(node *containers.RBNode[btrfsinspect.SysExtentCSum]) error {
		curEnd := curAddr + (btrfsvol.LogicalAddr(curSums.Len()/sumSize) * btrfssum.BlockSize)
		if node.Value.Sums.Addr != curEnd {
			if curSums.Len() > 0 {
				flattened.Runs = append(flattened.Runs, btrfssum.SumRun[btrfsvol.LogicalAddr]{
					ChecksumSize: sumSize,
					Addr:         curAddr,
					Sums:         btrfssum.ShortSum(curSums.String()),
				})
			}
			curAddr = node.Value.Sums.Addr
			curSums.Reset()
		}
		curSums.WriteString(string(node.Value.Sums.Sums))
		return nil
	})
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

	return flattened
}

func ListUnmappedLogicalRegions(fs *btrfs.FS, logicalSums btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr]) []btrfssum.SumRun[btrfsvol.LogicalAddr] {
	// There are a lot of ways this algorithm could be made
	// faster.
	var ret []btrfssum.SumRun[btrfsvol.LogicalAddr]
	var cur struct {
		Addr btrfsvol.LogicalAddr
		Size btrfsvol.AddrDelta
	}
	for _, run := range logicalSums.Runs {
		for addr := run.Addr; addr < run.Addr.Add(run.Size()); addr += btrfssum.BlockSize {
			if _, maxlen := fs.LV.Resolve(addr); maxlen < btrfssum.BlockSize {
				if cur.Size == 0 {
					cur.Addr = addr
					cur.Size = 0
				}
				cur.Size += btrfssum.BlockSize
			} else if cur.Size > 0 {
				begIdx := int(cur.Addr.Sub(run.Addr)/btrfssum.BlockSize) * run.ChecksumSize
				lenIdx := (int(cur.Size) / btrfssum.BlockSize) * run.ChecksumSize
				endIdx := begIdx + lenIdx
				ret = append(ret, btrfssum.SumRun[btrfsvol.LogicalAddr]{
					ChecksumSize: run.ChecksumSize,
					Addr:         cur.Addr,
					Sums:         run.Sums[begIdx:endIdx],
				})
				cur.Size = 0
			}
		}
		if cur.Size > 0 {
			begIdx := int(cur.Addr.Sub(run.Addr)/btrfssum.BlockSize) * run.ChecksumSize
			lenIdx := (int(cur.Size) / btrfssum.BlockSize) * run.ChecksumSize
			endIdx := begIdx + lenIdx
			ret = append(ret, btrfssum.SumRun[btrfsvol.LogicalAddr]{
				ChecksumSize: run.ChecksumSize,
				Addr:         cur.Addr,
				Sums:         run.Sums[begIdx:endIdx],
			})
			cur.Size = 0
		}
	}
	return ret
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
