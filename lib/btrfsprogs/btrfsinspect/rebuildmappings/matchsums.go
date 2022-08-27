// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func matchBlockGroupSums(ctx context.Context,
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
	bgMatches := make(map[btrfsvol.LogicalAddr][]btrfsvol.QualifiedPhysicalAddr)
	for i, bgLAddr := range maps.SortedKeys(blockgroups) {
		bgRun := bgSums[bgLAddr]
		if len(bgRun.Runs) == 0 {
			dlog.Errorf(ctx, "... (%v/%v) blockgroup[laddr=%v] can't be matched because it has 0 runs",
				i+1, len(bgSums), bgLAddr)
			continue
		}

		if err := WalkUnmappedPhysicalRegions(ctx, physicalSums, regions, func(devID btrfsvol.DeviceID, region btrfssum.SumRun[btrfsvol.PhysicalAddr]) error {
			matches, err := diskio.IndexAll[int64, btrfssum.ShortSum](region, bgRun)
			if err != nil {
				return err
			}
			for _, match := range matches {
				bgMatches[bgLAddr] = append(bgMatches[bgLAddr], btrfsvol.QualifiedPhysicalAddr{
					Dev:  devID,
					Addr: region.Addr + (btrfsvol.PhysicalAddr(match) * btrfssum.BlockSize),
				})
			}
			return nil
		}); err != nil {
			return err
		}

		lvl := dlog.LogLevelInfo
		if len(bgMatches[bgLAddr]) == 0 {
			lvl = dlog.LogLevelError
		}
		dlog.Logf(ctx, lvl, "... (%v/%v) blockgroup[laddr=%v] has %v matches based on %v%% coverage",
			i+1, len(bgSums), bgLAddr, len(bgMatches[bgLAddr]), int(100*bgRun.PctFull()))
	}
	dlog.Info(ctx, "... ... done searching")

	dlog.Info(ctx, "... Applying those mappings...")
	for _, bgLAddr := range maps.SortedKeys(bgMatches) {
		matches := bgMatches[bgLAddr]
		if len(matches) != 1 {
			continue
		}
		blockgroup := blockgroups[bgLAddr]
		mapping := btrfsvol.Mapping{
			LAddr:      blockgroup.LAddr,
			PAddr:      matches[0],
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
