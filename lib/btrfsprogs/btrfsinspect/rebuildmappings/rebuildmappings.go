// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func getNodeSize(fs *btrfs.FS) (btrfsvol.AddrDelta, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return 0, err
	}
	return btrfsvol.AddrDelta(sb.NodeSize), nil
}

func RebuildMappings(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) error {
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

	dlog.Infof(ctx, "1/6: Processing %d chunks...", numChunks)
	for _, devID := range devIDs {
		devResults := scanResults[devID]
		for _, chunk := range devResults.FoundChunks {
			for _, mapping := range chunk.Chunk.Mappings(chunk.Key) {
				if err := fs.LV.AddMapping(mapping); err != nil {
					dlog.Errorf(ctx, "... error: adding chunk: %v", err)
				}
			}
		}
	}
	dlog.Info(ctx, "... done processing chunks")

	dlog.Infof(ctx, "2/6: Processing %d device extents...", numDevExts)
	for _, devID := range devIDs {
		devResults := scanResults[devID]
		for _, ext := range devResults.FoundDevExtents {
			if err := fs.LV.AddMapping(ext.DevExt.Mapping(ext.Key)); err != nil {
				dlog.Errorf(ctx, "... error: adding devext: %v", err)
			}
		}
	}
	dlog.Info(ctx, "... done processing device extents")

	// Do the nodes "last" to avoid bloating the mappings table
	// too much.  (Because nodes are numerous and small, while the
	// others are few and large; so it is likely that many of the
	// nodes will be subsumed by other things.)
	dlog.Infof(ctx, "3/6: Processing %d nodes...", numNodes)
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
					dlog.Errorf(ctx, "... error: adding node ident: %v", err)
				}
			}
		}
	}
	dlog.Info(ctx, "... done processing nodes")

	// Use block groups to add missing flags (and as a hint to
	// combine node entries).
	dlog.Infof(ctx, "4/6: Processing %d block groups...", numBlockGroups)
	// First dedup them, because they change for allocations and
	// CoW means that they'll bounce around a lot, so you likely
	// have oodles of duplicates?
	bgs, err := DedupBlockGroups(scanResults)
	if err != nil {
		return err
	}
	dlog.Infof(ctx, "... de-duplicated to %d block groups", len(bgs))
	for _, bgLAddr := range maps.SortedKeys(bgs) {
		bg := bgs[bgLAddr]
		otherLAddr, otherPAddr := fs.LV.ResolveAny(bg.LAddr, bg.Size)
		if otherLAddr < 0 || otherPAddr.Addr < 0 {
			dlog.Errorf(ctx, "... error: could not pair blockgroup laddr=%v (size=%v flags=%v) with a mapping",
				bg.LAddr, bg.Size, bg.Flags)
			continue
		}

		offsetWithinChunk := otherLAddr.Sub(bg.LAddr)
		mapping := btrfsvol.Mapping{
			LAddr:      bg.LAddr,
			PAddr:      otherPAddr.Add(-offsetWithinChunk),
			Size:       bg.Size,
			SizeLocked: true,
			Flags: containers.Optional[btrfsvol.BlockGroupFlags]{
				OK:  true,
				Val: bg.Flags,
			},
		}
		if err := fs.LV.AddMapping(mapping); err != nil {
			dlog.Errorf(ctx, "... error: adding flags from blockgroup: %v", err)
			continue
		}
		delete(bgs, bgLAddr)
	}
	dlog.Info(ctx, "... done processing block groups")

	dlog.Infof(ctx, "5/6: Searching for %d block groups in checksum map (exact)...", len(bgs))
	physicalSums := ExtractPhysicalSums(scanResults)
	logicalSums := ExtractLogicalSums(ctx, scanResults)
	if err := matchBlockGroupSums(ctx, fs, bgs, physicalSums, logicalSums); err != nil {
		return err
	}
	dlog.Info(ctx, "... done searching for exact block groups")

	dlog.Infof(ctx, "6/6: Searching for %d block groups in checksum map (fuzzy)...", len(bgs))
	if err := fuzzyMatchBlockGroupSums(ctx, fs, bgs, physicalSums, logicalSums); err != nil {
		return err
	}
	dlog.Info(ctx, "... done searching for fuzzy block groups")

	dlog.Info(ctx, "report:")

	unmappedPhysicalRegions := ListUnmappedPhysicalRegions(fs)
	var unmappedPhysical btrfsvol.AddrDelta
	var numUnmappedPhysical int
	for _, devRegions := range unmappedPhysicalRegions {
		numUnmappedPhysical += len(devRegions)
		for _, region := range devRegions {
			unmappedPhysical += region.End.Sub(region.Beg)
		}
	}
	dlog.Infof(ctx, "... %d KiB of unmapped physical space (across %d regions)", int(unmappedPhysical/1024), numUnmappedPhysical)

	unmappedLogicalRegions := ListUnmappedLogicalRegions(fs, logicalSums)
	var unmappedLogical btrfsvol.AddrDelta
	for _, region := range unmappedLogicalRegions {
		unmappedLogical += region.Size()
	}
	dlog.Infof(ctx, "... %d KiB of unmapped summed logical space (across %d regions)", int(unmappedLogical/1024), len(unmappedLogicalRegions))

	var unmappedBlockGroups btrfsvol.AddrDelta
	for _, bg := range bgs {
		unmappedBlockGroups += bg.Size
	}
	dlog.Infof(ctx, "... %d KiB of unmapped block groups (across %d groups)", int(unmappedBlockGroups/1024), len(bgs))

	dlog.Info(ctx, "detailed report:")
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
