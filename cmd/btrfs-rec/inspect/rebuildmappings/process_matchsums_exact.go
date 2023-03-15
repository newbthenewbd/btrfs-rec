// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"

	"github.com/datawire/dlib/dlog"
	"golang.org/x/text/number"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func matchBlockGroupSumsExact(ctx context.Context,
	fs *btrfs.FS,
	blockgroups map[btrfsvol.LogicalAddr]BlockGroup,
	physicalSums map[btrfsvol.DeviceID]btrfssum.SumRun[btrfsvol.PhysicalAddr],
	logicalSums SumRunWithGaps[btrfsvol.LogicalAddr],
) error {
	regions := ListUnmappedPhysicalRegions(fs)
	numBlockgroups := len(blockgroups)
	for i, bgLAddr := range maps.SortedKeys(blockgroups) {
		blockgroup := blockgroups[bgLAddr]
		bgRun := SumsForLogicalRegion(logicalSums, blockgroup.LAddr, blockgroup.Size)
		if len(bgRun.Runs) == 0 {
			dlog.Errorf(ctx, "(%v/%v) blockgroup[laddr=%v] can't be matched because it has 0 runs",
				i+1, numBlockgroups, bgLAddr)
			continue
		}

		var matches []btrfsvol.QualifiedPhysicalAddr
		if err := WalkUnmappedPhysicalRegions(ctx, physicalSums, regions, func(devID btrfsvol.DeviceID, region btrfssum.SumRun[btrfsvol.PhysicalAddr]) error {
			rawMatches := indexAll[int, btrfssum.ShortSum](region, bgRun)
			for _, match := range rawMatches {
				matches = append(matches, btrfsvol.QualifiedPhysicalAddr{
					Dev:  devID,
					Addr: region.Addr + (btrfsvol.PhysicalAddr(match) * btrfssum.BlockSize),
				})
			}
			return nil
		}); err != nil {
			return err
		}

		lvl := dlog.LogLevelError
		if len(matches) == 1 {
			lvl = dlog.LogLevelInfo
		}
		dlog.Logf(ctx, lvl, "(%v/%v) blockgroup[laddr=%v] has %v matches based on %v coverage from %v runs",
			i+1, numBlockgroups, bgLAddr, len(matches), number.Percent(bgRun.PctFull()), len(bgRun.Runs))
		if len(matches) != 1 {
			continue
		}

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
			dlog.Errorf(ctx, "error: %v", err)
			continue
		}
		delete(blockgroups, bgLAddr)
	}
	return nil
}
