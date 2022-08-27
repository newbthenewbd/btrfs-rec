// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"sort"
	"strconv"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type fuzzyRecord struct {
	PAddr btrfsvol.QualifiedPhysicalAddr
	N     int64
	D     int64
}

func (r fuzzyRecord) Pct() float64 {
	return float64(r.N) / float64(r.D)
}

func (a fuzzyRecord) Cmp(b fuzzyRecord) int {
	aF := 1.0 - a.Pct()
	bF := 1.0 - b.Pct()
	switch {
	case aF < bF:
		return -1
	case aF > bF:
		return 1
	default:
		return 0
	}
}

func fuzzyMatchBlockGroupSums(ctx context.Context,
	fs *btrfs.FS,
	blockgroups map[btrfsvol.LogicalAddr]BlockGroup,
	physicalSums map[btrfsvol.DeviceID]btrfssum.SumRun[btrfsvol.PhysicalAddr],
	logicalSums btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr],
) error {
	dlog.Info(ctx, "... Pairing up blockgroups and sums...")
	bgSums := make(map[btrfsvol.LogicalAddr]btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr])
	for i, bgLAddr := range maps.SortedKeys(blockgroups) {
		blockgroup := blockgroups[bgLAddr]
		runs := SumsForLogicalRegion(logicalSums, blockgroup.LAddr, blockgroup.Size)
		bgSums[blockgroup.LAddr] = runs
		dlog.Infof(ctx, "... (%v/%v) blockgroup[laddr=%v] has %v runs covering %v%%",
			i+1, len(blockgroups), bgLAddr, len(runs.Runs), int(100*runs.PctFull()))
	}
	dlog.Info(ctx, "... ... done pairing")

	dlog.Info(ctx, "... Searching for unmapped blockgroups in unmapped physical regions...")
	regions := ListUnmappedPhysicalRegions(fs)
	bgMatches := make(map[btrfsvol.LogicalAddr]btrfsvol.QualifiedPhysicalAddr)
	for i, bgLAddr := range maps.SortedKeys(blockgroups) {
		bgRun := bgSums[bgLAddr]
		if len(bgRun.Runs) == 0 {
			dlog.Errorf(ctx, "... (%v/%v) blockgroup[laddr=%v] can't be matched because it has 0 runs",
				i+1, len(bgSums), bgLAddr)
			continue
		}

		best := lowestN[fuzzyRecord]{N: 5}
		if err := WalkUnmappedPhysicalRegions(ctx, physicalSums, regions, func(devID btrfsvol.DeviceID, region btrfssum.SumRun[btrfsvol.PhysicalAddr]) error {
			for paddr := region.Addr; true; paddr += btrfssum.BlockSize {
				n, d, err := pctMatch(ctx, region, paddr, bgRun)
				if err != nil {
					return err
				}
				if d == 0 {
					break
				}
				best.Insert(fuzzyRecord{
					PAddr: btrfsvol.QualifiedPhysicalAddr{
						Dev:  devID,
						Addr: paddr,
					},
					N: n,
					D: d,
				})
			}
			return nil
		}); err != nil {
			return err
		}

		var pcts []string
		for _, r := range best.Dat {
			pcts = append(pcts, strconv.Itoa(int(100*r.Pct()))+"%")
		}
		lvl := dlog.LogLevelError
		if len(best.Dat) > 0 && best.Dat[0].Pct() > 0.5 {
			bgMatches[bgLAddr] = best.Dat[0].PAddr
			lvl = dlog.LogLevelInfo
		}
		dlog.Logf(ctx, lvl, "... (%v/%v) blockgroup[laddr=%v] best %d matches are: %v",
			i+1, len(bgSums), bgLAddr, len(pcts), pcts)
	}
	dlog.Info(ctx, "... ... done searching")

	dlog.Info(ctx, "... Applying those mappings...")
	for _, bgLAddr := range maps.SortedKeys(bgMatches) {
		paddr := bgMatches[bgLAddr]
		blockgroup := blockgroups[bgLAddr]
		mapping := btrfsvol.Mapping{
			LAddr:      blockgroup.LAddr,
			PAddr:      paddr,
			Size:       blockgroup.Size,
			SizeLocked: true,
			Flags: containers.Optional[btrfsvol.BlockGroupFlags]{
				OK:  true,
				Val: blockgroup.Flags,
			},
		}
		if err := fs.LV.AddMapping(mapping); err != nil {
			dlog.Errorf(ctx, "... error: %v", err)
			continue
		}
		delete(blockgroups, bgLAddr)
	}
	dlog.Info(ctx, "... ... done applying")

	return nil
}

type lowestN[T containers.Ordered[T]] struct {
	N   int
	Dat []T
}

func (l *lowestN[T]) Insert(v T) {
	if len(l.Dat) < l.N {
		l.Dat = append(l.Dat, v)
	} else if v.Cmp(l.Dat[0]) < 0 {
		l.Dat[0] = v
	} else {
		return
	}
	sort.Slice(l.Dat, func(i, j int) bool {
		return l.Dat[i].Cmp(l.Dat[j]) < 0
	})
}

func pctMatch(
	ctx context.Context,
	searchspace btrfssum.SumRun[btrfsvol.PhysicalAddr], start btrfsvol.PhysicalAddr,
	pattern btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr],
) (n, d int64, err error) {
	if psize := searchspace.Size() - start.Sub(searchspace.Addr); psize < pattern.Size {
		return
	}
	err = pattern.Walk(ctx, func(laddr btrfsvol.LogicalAddr, lsum btrfssum.ShortSum) error {
		d++
		paddr := start.Add(laddr.Sub(pattern.Addr))
		psum, _ := searchspace.SumForAddr(paddr)
		if psum == lsum {
			n++
		}
		return nil
	})
	return
}
