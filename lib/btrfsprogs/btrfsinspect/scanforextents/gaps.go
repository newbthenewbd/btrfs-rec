// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package scanforextents

import (
	"context"
	"sort"

	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
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
	gaps []PhysicalGap, blockSize btrfsvol.PhysicalAddr,
	progress func(cur, total int64),
	main func(btrfsvol.PhysicalAddr) error,
) error {
	var totalBlocks int64
	for _, gap := range gaps {
		for paddr := roundUp(gap.Beg, blockSize); paddr+blockSize <= gap.End; paddr += blockSize {
			totalBlocks++
		}
	}

	var curBlock int64
	for _, gap := range gaps {
		for paddr := roundUp(gap.Beg, blockSize); paddr+blockSize <= gap.End; paddr += blockSize {
			if err := ctx.Err(); err != nil {
				return err
			}
			progress(curBlock, totalBlocks)
			curBlock++
			if err := main(paddr); err != nil {
				return err
			}
		}
	}
	progress(curBlock, totalBlocks)
	return nil
}
