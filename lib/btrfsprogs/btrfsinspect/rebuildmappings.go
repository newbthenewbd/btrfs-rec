// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsinspect

import (
	"context"
	"sort"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func (found ScanOneDeviceResult) AddToLV(ctx context.Context, fs *btrfs.FS, dev *btrfs.Device) {
	sb, _ := dev.Superblock()

	total := len(found.FoundChunks) + len(found.FoundDevExtents)
	for _, paddrs := range found.FoundNodes {
		total += len(paddrs)
	}
	lastProgress := -1
	done := 0
	printProgress := func() {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastProgress || done == total {
			dlog.Infof(ctx, "... dev[%q] added %v%% of the mappings (%v/%v=>%v)",
				dev.Name(), pct, done, total, len(fs.LV.Mappings()))
			lastProgress = pct
		}
	}
	printProgress()

	for _, chunk := range found.FoundChunks {
		for _, mapping := range chunk.Chunk.Mappings(chunk.Key) {
			if err := fs.LV.AddMapping(mapping); err != nil {
				dlog.Errorf(ctx, "... dev[%q] error: adding chunk: %v",
					dev.Name(), err)
			}
			done++
			printProgress()
		}
	}

	for _, ext := range found.FoundDevExtents {
		if err := fs.LV.AddMapping(ext.DevExt.Mapping(ext.Key)); err != nil {
			dlog.Errorf(ctx, "... dev[%q] error: adding devext: %v",
				dev.Name(), err)
		}
		done++
		printProgress()
	}

	// Do the nodes last to avoid bloating the mappings table too
	// much. (Because nodes are numerous and small, while the
	// others are few and large; so it is likely that many of the
	// nodes will be subsumed by other things.)
	//
	// Sort them so that progress numbers are predictable.
	for _, laddr := range maps.SortedKeys(found.FoundNodes) {
		for _, paddr := range found.FoundNodes[laddr] {
			if err := fs.LV.AddMapping(btrfsvol.Mapping{
				LAddr: laddr,
				PAddr: btrfsvol.QualifiedPhysicalAddr{
					Dev:  sb.DevItem.DevID,
					Addr: paddr,
				},
				Size:       btrfsvol.AddrDelta(sb.NodeSize),
				SizeLocked: false,
			}); err != nil {
				dlog.Errorf(ctx, "... dev[%q] error: adding node ident: %v",
					dev.Name(), err)
			}
			done++
			printProgress()
		}
	}

	// Use block groups to add missing flags (and as a hint to
	// combine node entries).
	//
	// First dedup them, because they change for allocations and
	// CoW means that they'll bounce around a lot, so you likely
	// have oodles of duplicates?
	type blockgroup struct {
		LAddr btrfsvol.LogicalAddr
		Size  btrfsvol.AddrDelta
		Flags btrfsvol.BlockGroupFlags
	}
	bgsSet := make(map[blockgroup]struct{})
	for _, bg := range found.FoundBlockGroups {
		bgsSet[blockgroup{
			LAddr: btrfsvol.LogicalAddr(bg.Key.ObjectID),
			Size:  btrfsvol.AddrDelta(bg.Key.Offset),
			Flags: bg.BG.Flags,
		}] = struct{}{}
	}
	bgsOrdered := maps.Keys(bgsSet)
	sort.Slice(bgsOrdered, func(i, j int) bool {
		return bgsOrdered[i].LAddr < bgsOrdered[j].LAddr
	})
	for _, bg := range bgsOrdered {
		otherLAddr, otherPAddr := fs.LV.ResolveAny(bg.LAddr, bg.Size)
		if otherLAddr < 0 || otherPAddr.Addr < 0 {
			dlog.Errorf(ctx, "... dev[%q] error: could not pair blockgroup laddr=%v (size=%v flags=%v) with a mapping",
				dev.Name(), bg.LAddr, bg.Size, bg.Flags)
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
			dlog.Errorf(ctx, "... dev[%q] error: adding flags from blockgroup: %v",
				dev.Name(), err)
		}
	}
}
