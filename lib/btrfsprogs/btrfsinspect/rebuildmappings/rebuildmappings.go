// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"fmt"
	"sort"

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

type blockgroup struct {
	LAddr btrfsvol.LogicalAddr
	Size  btrfsvol.AddrDelta
	Flags btrfsvol.BlockGroupFlags
}

func dedupBlockGroups(scanResults btrfsinspect.ScanDevicesResult) []blockgroup {
	bgsSet := make(map[blockgroup]struct{})
	for _, devResults := range scanResults {
		for _, bg := range devResults.FoundBlockGroups {
			bgsSet[blockgroup{
				LAddr: btrfsvol.LogicalAddr(bg.Key.ObjectID),
				Size:  btrfsvol.AddrDelta(bg.Key.Offset),
				Flags: bg.BG.Flags,
			}] = struct{}{}
		}
	}
	bgsOrdered := maps.Keys(bgsSet)
	sort.Slice(bgsOrdered, func(i, j int) bool {
		return bgsOrdered[i].LAddr < bgsOrdered[j].LAddr
	})
	return bgsOrdered
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
	dlog.Infof(ctx, "plan: 1/5 process %d chunks", numChunks)
	dlog.Infof(ctx, "plan: 2/5 process %d device extents", numDevExts)
	dlog.Infof(ctx, "plan: 3/5 process %d nodes", numNodes)
	dlog.Infof(ctx, "plan: 4/5 process %d block groups", numBlockGroups)
	dlog.Infof(ctx, "plan: 5/5 process sums")

	dlog.Infof(ctx, "1/5: Processing %d chunks...", numChunks)
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

	dlog.Infof(ctx, "2/5: Processing %d device extents...", numDevExts)
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
	dlog.Infof(ctx, "3/5: Processing %d nodes...", numNodes)
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
	dlog.Infof(ctx, "4/5: Processing %d block groups...", numBlockGroups)
	// First dedup them, because they change for allocations and
	// CoW means that they'll bounce around a lot, so you likely
	// have oodles of duplicates?
	bgsOrdered := dedupBlockGroups(scanResults)
	for _, bg := range bgsOrdered {
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
		}
	}
	dlog.Info(ctx, "... done processing block groups")

	dlog.Infof(ctx, "5/5: Processing sums: TODO")
	return nil
}
