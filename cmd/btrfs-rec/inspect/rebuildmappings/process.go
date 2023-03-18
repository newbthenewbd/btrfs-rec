// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package rebuildmappings is the guts of the `btrfs-rec inspect
// rebuild-mappings` command, which rebuilds broken
// chunk/dev-extent/blockgroup trees.
package rebuildmappings

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func getNodeSize(fs *btrfs.FS) (btrfsvol.AddrDelta, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return 0, err
	}
	return btrfsvol.AddrDelta(sb.NodeSize), nil
}

func RebuildMappings(ctx context.Context, fs *btrfs.FS, scanResults ScanDevicesResult) error {
	nodeSize, err := getNodeSize(fs)
	if err != nil {
		return err
	}

	var numChunks, numDevExts, numBlockGroups, numNodes int
	devIDs := maps.SortedKeys(scanResults)
	devices := fs.LV.PhysicalVolumes()
	for _, devID := range devIDs {
		if _, ok := devices[devID]; !ok {
			return fmt.Errorf("device ID %v mentioned in scan results is not part of the filesystem", devID)
		}
		devResults := scanResults[devID]
		numChunks += len(devResults.FoundChunks)
		numDevExts += len(devResults.FoundDevExtents)
		numBlockGroups += len(devResults.FoundBlockGroups)
		for _, paddrs := range devResults.FoundNodes {
			numNodes += len(paddrs)
		}
	}
	dlog.Infof(ctx, "plan: 1/6 process %d chunks", numChunks)
	dlog.Infof(ctx, "plan: 2/6 process %d device extents", numDevExts)
	dlog.Infof(ctx, "plan: 3/6 process %d nodes", numNodes)
	dlog.Infof(ctx, "plan: 4/6 process %d block groups", numBlockGroups)
	dlog.Infof(ctx, "plan: 5/6 search for block groups in checksum map (exact)")
	dlog.Infof(ctx, "plan: 6/6 search for block groups in checksum map (fuzzy)")

	_ctx := ctx
	ctx = dlog.WithField(_ctx, "btrfs.inspect.rebuild-mappings.process.step", "1/6")
	dlog.Infof(_ctx, "1/6: Processing %d chunks...", numChunks)
	for _, devID := range devIDs {
		devResults := scanResults[devID]
		for _, chunk := range devResults.FoundChunks {
			for _, mapping := range chunk.Chunk.Mappings(chunk.Key) {
				if err := fs.LV.AddMapping(mapping); err != nil {
					dlog.Errorf(ctx, "error: adding chunk: %v", err)
				}
			}
		}
	}
	dlog.Info(_ctx, "... done processing chunks")

	ctx = dlog.WithField(_ctx, "btrfs.inspect.rebuild-mappings.process.step", "2/6")
	dlog.Infof(_ctx, "2/6: Processing %d device extents...", numDevExts)
	for _, devID := range devIDs {
		devResults := scanResults[devID]
		for _, ext := range devResults.FoundDevExtents {
			if err := fs.LV.AddMapping(ext.DevExt.Mapping(ext.Key)); err != nil {
				dlog.Errorf(ctx, "error: adding devext: %v", err)
			}
		}
	}
	dlog.Info(_ctx, "... done processing device extents")

	// Do the nodes "last" to avoid bloating the mappings table
	// too much.  (Because nodes are numerous and small, while the
	// others are few and large; so it is likely that many of the
	// nodes will be subsumed by other things.)
	ctx = dlog.WithField(_ctx, "btrfs.inspect.rebuild-mappings.process.step", "3/6")
	dlog.Infof(_ctx, "3/6: Processing %d nodes...", numNodes)
	for _, devID := range devIDs {
		devResults := scanResults[devID]
		// Sort them so that progress numbers are predictable.
		for _, laddr := range maps.SortedKeys(devResults.FoundNodes) {
			for _, paddr := range devResults.FoundNodes[laddr] {
				if err := fs.LV.AddMapping(btrfsvol.Mapping{
					LAddr: laddr,
					PAddr: btrfsvol.QualifiedPhysicalAddr{
						Dev:  devID,
						Addr: paddr,
					},
					Size:       nodeSize,
					SizeLocked: false,
				}); err != nil {
					dlog.Errorf(ctx, "error: adding node ident: %v", err)
				}
			}
		}
	}
	dlog.Info(_ctx, "... done processing nodes")

	// Use block groups to add missing flags (and as a hint to
	// combine node entries).
	ctx = dlog.WithField(_ctx, "btrfs.inspect.rebuild-mappings.process.step", "4/6")
	dlog.Infof(_ctx, "4/6: Processing %d block groups...", numBlockGroups)
	// First dedup them, because they change for allocations and
	// CoW means that they'll bounce around a lot, so you likely
	// have oodles of duplicates?
	bgs, err := dedupedBlockGroups(scanResults)
	if err != nil {
		return err
	}
	dlog.Infof(ctx, "... de-duplicated to %d block groups", len(bgs))
	for _, bgLAddr := range maps.SortedKeys(bgs) {
		bg := bgs[bgLAddr]
		otherLAddr, otherPAddr := fs.LV.ResolveAny(bg.LAddr, bg.Size)
		if otherLAddr < 0 || otherPAddr.Addr < 0 {
			dlog.Errorf(ctx, "error: could not pair blockgroup laddr=%v (size=%v flags=%v) with a mapping",
				bg.LAddr, bg.Size, bg.Flags)
			continue
		}

		offsetWithinChunk := otherLAddr.Sub(bg.LAddr)
		mapping := btrfsvol.Mapping{
			LAddr:      bg.LAddr,
			PAddr:      otherPAddr.Add(-offsetWithinChunk),
			Size:       bg.Size,
			SizeLocked: true,
			Flags:      containers.OptionalValue(bg.Flags),
		}
		if err := fs.LV.AddMapping(mapping); err != nil {
			dlog.Errorf(ctx, "error: adding flags from blockgroup: %v", err)
			continue
		}
		delete(bgs, bgLAddr)
	}
	dlog.Info(_ctx, "... done processing block groups")

	// More than once, I've been tempted to get rid of this exact-search step and just have the fuzzy-search step.
	// After all, looking at the timestamps in the log, it's faster per blockgroup!  For some background, the big-O
	// for each (per blockgroup) looks like:
	//
	//  - exact-search: O(bgSize+physicalBlocks)
	//  - fuzzy-search: O(bgSize*physicalBlocks) worst-case; O(bgSize*log(physicalBlocks)) expected
	//
	// The fuzzy-search is only fast because the exact-search is so good at getting `physicalBlocks` down.
	// Empirically: if I remove the exact-search step, then the fuzzy-match step is more than an order of magnitude
	// slower.
	ctx = dlog.WithField(_ctx, "btrfs.inspect.rebuild-mappings.process.step", "5/6")
	dlog.Infof(_ctx, "5/6: Searching for %d block groups in checksum map (exact)...", len(bgs))
	physicalSums := extractPhysicalSums(scanResults)
	logicalSums := extractLogicalSums(ctx, scanResults)
	if err := matchBlockGroupSumsExact(ctx, fs, bgs, physicalSums, logicalSums); err != nil {
		return err
	}
	dlog.Info(ctx, "... done searching for exact block groups")

	ctx = dlog.WithField(_ctx, "btrfs.inspect.rebuild-mappings.process.step", "6/6")
	dlog.Infof(_ctx, "6/6: Searching for %d block groups in checksum map (fuzzy)...", len(bgs))
	if err := matchBlockGroupSumsFuzzy(ctx, fs, bgs, physicalSums, logicalSums); err != nil {
		return err
	}
	dlog.Info(_ctx, "... done searching for fuzzy block groups")

	ctx = dlog.WithField(_ctx, "btrfs.inspect.rebuild-mappings.process.step", "report")
	dlog.Info(_ctx, "report:")

	unmappedPhysicalRegions := listUnmappedPhysicalRegions(fs)
	var unmappedPhysical btrfsvol.AddrDelta
	var numUnmappedPhysical int
	for _, devRegions := range unmappedPhysicalRegions {
		numUnmappedPhysical += len(devRegions)
		for _, region := range devRegions {
			unmappedPhysical += region.End.Sub(region.Beg)
		}
	}
	dlog.Infof(ctx, "... %d of unmapped physical space (across %d regions)", textui.IEC(unmappedPhysical, "B"), numUnmappedPhysical)

	unmappedLogicalRegions := listUnmappedLogicalRegions(fs, logicalSums)
	var unmappedLogical btrfsvol.AddrDelta
	for _, region := range unmappedLogicalRegions {
		unmappedLogical += region.Size()
	}
	dlog.Infof(ctx, "... %d of unmapped summed logical space (across %d regions)", textui.IEC(unmappedLogical, "B"), len(unmappedLogicalRegions))

	var unmappedBlockGroups btrfsvol.AddrDelta
	for _, bg := range bgs {
		unmappedBlockGroups += bg.Size
	}
	dlog.Infof(ctx, "... %d of unmapped block groups (across %d groups)", textui.IEC(unmappedBlockGroups, "B"), len(bgs))

	dlog.Info(_ctx, "detailed report:")
	for _, devID := range maps.SortedKeys(unmappedPhysicalRegions) {
		for _, region := range unmappedPhysicalRegions[devID] {
			dlog.Infof(ctx, "... unmapped physical region: dev=%v beg=%v end=%v (size=%v)",
				devID, region.Beg, region.End, region.End.Sub(region.Beg))
		}
	}
	for _, region := range unmappedLogicalRegions {
		dlog.Infof(ctx, "... umapped summed logical region:  beg=%v end=%v (size=%v)",
			region.Addr, region.Addr.Add(region.Size()), region.Size())
	}
	for _, laddr := range maps.SortedKeys(bgs) {
		bg := bgs[laddr]
		dlog.Infof(ctx, "... umapped block group:            beg=%v end=%v (size=%v) flags=%v",
			bg.LAddr, bg.LAddr.Add(bg.Size), bg.Size, bg.Flags)
	}

	return nil
}
