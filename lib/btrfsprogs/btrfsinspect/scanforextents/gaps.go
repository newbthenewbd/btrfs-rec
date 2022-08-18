// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package scanforextents

import (
	"context"
	"sort"

	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type PhysicalGap struct {
	Beg, End btrfsvol.PhysicalAddr
}

func ListPhysicalGaps(fs *btrfs.FS) map[btrfsvol.DeviceID][]PhysicalGap {
	gaps := make(map[btrfsvol.DeviceID][]PhysicalGap)
	pos := make(map[btrfsvol.DeviceID]btrfsvol.PhysicalAddr)
	mappings := fs.LV.Mappings()
	sort.Slice(mappings, func(i, j int) bool {
		return mappings[i].PAddr.Cmp(mappings[j].PAddr) < 0
	})
	for _, mapping := range mappings {
		if pos[mapping.PAddr.Dev] < mapping.PAddr.Addr {
			gaps[mapping.PAddr.Dev] = append(gaps[mapping.PAddr.Dev], PhysicalGap{
				Beg: pos[mapping.PAddr.Dev],
				End: mapping.PAddr.Addr,
			})
		}
		if pos[mapping.PAddr.Dev] < mapping.PAddr.Addr.Add(mapping.Size) {
			pos[mapping.PAddr.Dev] = mapping.PAddr.Addr.Add(mapping.Size)
		}
	}
	for devID, dev := range fs.LV.PhysicalVolumes() {
		devSize := dev.Size()
		if pos[devID] < devSize {
			gaps[devID] = append(gaps[devID], PhysicalGap{
				Beg: pos[devID],
				End: devSize,
			})
		}
	}
	return gaps
}

func roundUp[T constraints.Integer](x, multiple T) T {
	return ((x + multiple - 1) / multiple) * multiple
}

func WalkGaps(ctx context.Context,
	sums AllSums, gaps map[btrfsvol.DeviceID][]PhysicalGap,
	fn func(btrfsvol.DeviceID, SumRun[btrfsvol.PhysicalAddr]) error,
) error {
	for _, devID := range maps.SortedKeys(gaps) {
		for _, gap := range gaps[devID] {
			if err := ctx.Err(); err != nil {
				return err
			}
			begAddr := roundUp(gap.Beg, btrfsitem.CSumBlockSize)
			begOff := int(begAddr/btrfsitem.CSumBlockSize) * sums.Physical[devID].ChecksumSize
			endOff := int(gap.End/btrfsitem.CSumBlockSize) * sums.Physical[devID].ChecksumSize
			if err := fn(devID, SumRun[btrfsvol.PhysicalAddr]{
				ChecksumSize: sums.Physical[devID].ChecksumSize,
				Addr:         begAddr,
				Sums:         sums.Physical[devID].Sums[begOff:endOff],
			}); err != nil {
				return err
			}
		}
	}
	return nil
}
