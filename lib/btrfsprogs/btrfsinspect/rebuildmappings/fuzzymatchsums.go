// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"sort"

	"github.com/datawire/dlib/dlog"
	"golang.org/x/text/number"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

var minFuzzyPct = textui.Tunable(0.5)

type fuzzyRecord struct {
	PAddr btrfsvol.QualifiedPhysicalAddr
	N     int
}

func (a fuzzyRecord) Cmp(b fuzzyRecord) int {
	switch {
	case a.N < b.N:
		return -1
	case a.N > b.N:
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
	_ctx := ctx

	ctx = dlog.WithField(_ctx, "btrfsinspect.rebuild-mappings.substep", "indexing")
	dlog.Info(ctx, "Indexing physical regions...") // O(m)
	regions := ListUnmappedPhysicalRegions(fs)
	physicalIndex := make(map[btrfssum.ShortSum][]btrfsvol.QualifiedPhysicalAddr)
	if err := WalkUnmappedPhysicalRegions(ctx, physicalSums, regions, func(devID btrfsvol.DeviceID, region btrfssum.SumRun[btrfsvol.PhysicalAddr]) error {
		return region.Walk(ctx, func(paddr btrfsvol.PhysicalAddr, sum btrfssum.ShortSum) error {
			physicalIndex[sum] = append(physicalIndex[sum], btrfsvol.QualifiedPhysicalAddr{
				Dev:  devID,
				Addr: paddr,
			})
			return nil
		})
	}); err != nil {
		return err
	}
	dlog.Info(ctx, "... done indexing")

	ctx = dlog.WithField(_ctx, "btrfsinspect.rebuild-mappings.substep", "searching")
	dlog.Info(ctx, "Searching...")
	numBlockgroups := len(blockgroups)
	for i, bgLAddr := range maps.SortedKeys(blockgroups) {
		blockgroup := blockgroups[bgLAddr]
		bgRun := SumsForLogicalRegion(logicalSums, blockgroup.LAddr, blockgroup.Size)

		d := bgRun.NumSums()
		matches := make(map[btrfsvol.QualifiedPhysicalAddr]int)
		if err := bgRun.Walk(ctx, func(laddr btrfsvol.LogicalAddr, sum btrfssum.ShortSum) error { // O(n*…
			off := laddr.Sub(bgRun.Addr)
			for _, paddr := range physicalIndex[sum] { // …log(m)) expected but …m) worst
				key := btrfsvol.QualifiedPhysicalAddr{
					Dev:  paddr.Dev,
					Addr: paddr.Addr.Add(-off),
				}
				matches[key]++
			}
			return nil
		}); err != nil {
			return err
		}

		best := lowestN[fuzzyRecord]{N: 2}
		for paddr, n := range matches { // O(m)
			best.Insert(fuzzyRecord{
				PAddr: paddr,
				N:     d - n,
			})
		}

		var apply bool
		var matchesStr string
		switch len(best.Dat) {
		case 0: // can happen if there are no sums in the run
			matchesStr = ""
		case 1: // not sure how this can happen, but whatev
			pct := float64(d-best.Dat[0].N) / float64(d)
			matchesStr = textui.Sprintf("%v", number.Percent(pct))
			apply = pct > minFuzzyPct
		case 2:
			pct := float64(d-best.Dat[0].N) / float64(d)
			pct2 := float64(d-best.Dat[1].N) / float64(d)
			matchesStr = textui.Sprintf("best=%v secondbest=%v", number.Percent(pct), number.Percent(pct2))
			apply = pct > minFuzzyPct && pct2 < minFuzzyPct
		}
		lvl := dlog.LogLevelError
		if apply {
			lvl = dlog.LogLevelInfo
		}
		dlog.Logf(ctx, lvl, "(%v/%v) blockgroup[laddr=%v] matches=[%s]; bestpossible=%v%% (based on %v runs)",
			i+1, numBlockgroups, bgLAddr, matchesStr, int(100*bgRun.PctFull()), len(bgRun.Runs))
		if !apply {
			continue
		}

		mapping := btrfsvol.Mapping{
			LAddr:      blockgroup.LAddr,
			PAddr:      best.Dat[0].PAddr,
			Size:       blockgroup.Size,
			SizeLocked: true,
			Flags: containers.Optional[btrfsvol.BlockGroupFlags]{
				OK:  true,
				Val: blockgroup.Flags,
			},
		}
		if err := fs.LV.AddMapping(mapping); err != nil {
			dlog.Errorf(ctx, "error: %v", err)
			continue
		}
		delete(blockgroups, bgLAddr)
	}
	dlog.Info(ctx, "done searching")

	return nil
}

type lowestN[T containers.Ordered[T]] struct {
	N   int
	Dat []T
}

func (l *lowestN[T]) Insert(v T) {
	switch {
	case len(l.Dat) < l.N:
		l.Dat = append(l.Dat, v)
	case v.Cmp(l.Dat[0]) < 0:
		l.Dat[0] = v
	default:
		return
	}
	sort.Slice(l.Dat, func(i, j int) bool {
		return l.Dat[i].Cmp(l.Dat[j]) < 0
	})
}
