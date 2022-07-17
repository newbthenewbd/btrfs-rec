// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package scanforextents

import (
	"context"
	"errors"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func ScanForExtents(ctx context.Context, fs *btrfs.FS, blockgroups map[btrfsvol.LogicalAddr]BlockGroup, sums AllSums) error {
	dlog.Info(ctx, "Pairing up blockgroups and sums...")
	bgSums := make(map[btrfsvol.LogicalAddr][]SumRun[btrfsvol.LogicalAddr])
	for i, bgLAddr := range maps.SortedKeys(blockgroups) {
		blockgroup := blockgroups[bgLAddr]
		for laddr := blockgroup.LAddr; laddr < blockgroup.LAddr.Add(blockgroup.Size); {
			run, next, ok := sums.RunForLAddr(laddr)
			if !ok {
				laddr = next
				continue
			}
			off := int((laddr-run.Addr)/csumBlockSize) * run.ChecksumSize
			deltaAddr := slices.Min[btrfsvol.AddrDelta](
				blockgroup.Size-laddr.Sub(blockgroup.LAddr),
				btrfsvol.AddrDelta((len(run.Sums)-off)/run.ChecksumSize)*csumBlockSize)
			deltaOff := int(deltaAddr/csumBlockSize) * run.ChecksumSize
			bgSums[blockgroup.LAddr] = append(bgSums[blockgroup.LAddr], SumRun[btrfsvol.LogicalAddr]{
				ChecksumSize: run.ChecksumSize,
				Addr:         laddr,
				Sums:         run.Sums[off : off+deltaOff],
			})
			laddr = laddr.Add(deltaAddr)
		}
		dlog.Infof(ctx, "... (%v/%v) blockgroup[laddr=%v] has %v runs",
			i+1, len(blockgroups), bgLAddr, len(bgSums[blockgroup.LAddr]))
	}
	dlog.Info(ctx, "... done pairing")

	dlog.Info(ctx, "Searching for unmapped blockgroups in unmapped regions...")
	gaps := ListPhysicalGaps(fs)
	bgMatches := make(map[btrfsvol.LogicalAddr][]btrfsvol.QualifiedPhysicalAddr)
	for i, bgLAddr := range maps.SortedKeys(blockgroups) {
		bgRuns := bgSums[bgLAddr]
		if len(bgRuns) != 1 {
			// TODO(lukeshu): We aught to handle this rather than erroring and skipping
			// it.
			dlog.Errorf(ctx, "blockgroup laddr=%v has holes (%v runs)", bgLAddr, len(bgRuns))
			continue
		}
		bgRun := bgRuns[0]

		if err := WalkGaps(ctx, sums, gaps, func(devID btrfsvol.DeviceID, gap SumRun[btrfsvol.PhysicalAddr]) error {
			matches, err := diskio.IndexAll[int64, ShortSum](gap, bgRun)
			if err != nil {
				return err
			}
			for _, match := range matches {
				bgMatches[bgLAddr] = append(bgMatches[bgLAddr], btrfsvol.QualifiedPhysicalAddr{
					Dev:  devID,
					Addr: gap.Addr + (btrfsvol.PhysicalAddr(match) * csumBlockSize),
				})
			}
			return nil
		}); err != nil {
			return err
		}
		dlog.Infof(ctx, "... (%v/%v) blockgroup[laddr=%v] has %v matches",
			i+1, len(bgSums), bgLAddr, len(bgMatches[bgLAddr]))
	}
	dlog.Info(ctx, "... done searching")

	dlog.Info(ctx, "Applying those mappings...")
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
			dlog.Error(ctx, err)
		}
	}
	dlog.Info(ctx, "... done applying")

	dlog.Info(ctx, "Reverse-indexing remaining unmapped logical sums...")
	sum2laddrs := make(map[ShortSum][]btrfsvol.LogicalAddr)
	var numUnmappedBlocks int64
	if err := sums.WalkLogical(ctx, func(laddr btrfsvol.LogicalAddr, sum ShortSum) error {
		var dat [csumBlockSize]byte
		if _, err := fs.ReadAt(dat[:], laddr); err != nil {
			if errors.Is(err, btrfsvol.ErrCouldNotMap) {
				sum2laddrs[sum] = append(sum2laddrs[sum], laddr)
				numUnmappedBlocks++
				return nil
			}
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	dlog.Infof(ctx, "... done reverse-indexing; %v still unmapped logical sums",
		numUnmappedBlocks)

	/* TODO

	dlog.Info(ctx, "Cross-referencing sums to re-construct mappings...")
	newMappings := &ExtentMappings{
		InLV:          &fs.LV,
		InBlockGroups: blockgroups,
		InSums:        sums,
		InReverseSums: sum2laddrs,
	}
	gaps := ListPhysicalGaps(fs)
	for _, devID := range maps.SortedKeys(gaps) {
		if err := newMappings.ScanOneDev(ctx,
			devID, devs[devID].Name(),
			gaps[devID],
		); err != nil {
			return err
		}
	}
	dlog.Info(ctx, "... done cross-referencing")

	dlog.Info(ctx, "Applying those mappings...")
	for laddr, mappings := range newMappings.OutSum2mappings {
		if len(mappings) > 1 {
			dlog.Errorf(ctx, "multiple possibilities for laddr=%v :", laddr)
			for _, mapping := range mappings {
				dlog.Errorf(ctx, "  - %#v", *mapping)
			}
			continue
		}
		if err := fs.LV.AddMapping(*mappings[0]); err != nil {
			dlog.Error(ctx, err)
		}
	}
	dlog.Info(ctx, "... done applying")

	*/

	return nil
}

/*

type ExtentMappings struct {
	// input
	InLV          *btrfsvol.LogicalVolume[*btrfs.Device]
	InBlockGroups *BlockGroupTree
	InSums        AllSums
	InReverseSums map[ShortSum][]btrfsvol.LogicalAddr

	// state
	internedMappings map[btrfsvol.Mapping]*btrfsvol.Mapping

	// output
	OutSum2mappings map[ShortSum][]*btrfsvol.Mapping
}

func (em *ExtentMappings) considerMapping(ctx context.Context, laddr btrfsvol.LogicalAddr, paddr btrfsvol.QualifiedPhysicalAddr) (btrfsvol.Mapping, bool) {
	blockgroup := LookupBlockGroup(em.InBlockGroups, laddr, csumBlockSize)
	if blockgroup == nil {
		return btrfsvol.Mapping{
			LAddr: laddr,
			PAddr: paddr,
			Size:  csumBlockSize,
		}, true
	}
	mapping := btrfsvol.Mapping{
		LAddr: blockgroup.LAddr,
		PAddr: btrfsvol.QualifiedPhysicalAddr{
			Dev:  paddr.Dev,
			Addr: paddr.Addr.Add(laddr.Sub(blockgroup.LAddr)),
		},
		Size:       blockgroup.Size,
		SizeLocked: true,
		Flags: containers.Optional[btrfsvol.BlockGroupFlags]{
			OK:  true,
			Val: blockgroup.Flags,
		},
	}
	if !em.InLV.CouldAddMapping(mapping) {
		return btrfsvol.Mapping{}, false
	}

	for offset := btrfsvol.AddrDelta(0); offset <= mapping.Size; offset += csumBlockSize {
		expCSum, ok := em.InSums.SumForLAddr(mapping.LAddr.Add(offset))
		if !ok {
			continue
		}
		actCSum, _ := em.InSums.SumForPAddr(mapping.PAddr.Add(offset))
		if actCSum != expCSum {
			return btrfsvol.Mapping{}, false
		}
	}
	return mapping, true
}

func (em *ExtentMappings) addMapping(sum ShortSum, mapping btrfsvol.Mapping) {
	interned := em.internedMappings[mapping]
	if interned == nil {
		interned = &mapping
		em.internedMappings[mapping] = interned
	}

	em.OutSum2mappings[sum] = append(em.OutSum2mappings[sum], interned)
}

func (em *ExtentMappings) ScanOneDev(
	ctx context.Context,
	devID btrfsvol.DeviceID, devName string,
	gaps []PhysicalGap,
) error {
	if em.internedMappings == nil {
		em.internedMappings = make(map[btrfsvol.Mapping]*btrfsvol.Mapping)
	}
	if em.OutSum2mappings == nil {
		em.OutSum2mappings = make(map[ShortSum][]*btrfsvol.Mapping)
	}

	dlog.Infof(ctx, "... dev[%q] Scanning for extents...", devName)

	var totalMappings int
	_ = WalkGaps(ctx, gaps, csumBlockSize,
		func(_, _ int64) {},
		func(paddr btrfsvol.PhysicalAddr) error {
			qpaddr := btrfsvol.QualifiedPhysicalAddr{
				Dev:  devID,
				Addr: paddr,
			}
			sum, _ := em.InSums.SumForPAddr(qpaddr)
			totalMappings += len(em.InReverseSums[sum])
			return nil
		},
	)

	lastProgress := -1
	considered := 0
	accepted := 0
	progress := func() {
		pct := int(100 * 10000 * float64(considered) / float64(totalMappings))
		if pct != lastProgress || considered == totalMappings {
			dlog.Infof(ctx, "... dev[%q] scanned %v%% (considered %v/%v pairings, accepted %v)",
				devName, float64(pct)/10000.0, considered, totalMappings, accepted)
			lastProgress = pct
		}
	}
	return WalkGaps(ctx, gaps, csumBlockSize,
		func(_, _ int64) {
			progress()
		},
		func(paddr btrfsvol.PhysicalAddr) error {
			qpaddr := btrfsvol.QualifiedPhysicalAddr{
				Dev:  devID,
				Addr: paddr,
			}
			sum, _ := em.InSums.SumForPAddr(qpaddr)
			for i, laddr := range em.InReverseSums[sum] {
				if i%100 == 0 {
					if err := ctx.Err(); err != nil {
						return err
					}
				}
				mapping, ok := em.considerMapping(ctx, laddr, qpaddr)
				considered++
				if !ok {
					continue
				}
				em.addMapping(sum, mapping)
				accepted++
				progress()
			}

			return nil
		},
	)
}

*/
