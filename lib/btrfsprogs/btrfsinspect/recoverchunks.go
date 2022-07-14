// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsinspect

import (
	"context"
	"sort"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type ScanOneDevResult struct {
	FoundNodes       map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr
	FoundChunks      []btrfs.SysChunk
	FoundBlockGroups []SysBlockGroup
	FoundDevExtents  []SysDevExtent
}

type SysBlockGroup struct {
	Key btrfs.Key
	BG  btrfsitem.BlockGroup
}

type SysDevExtent struct {
	Key    btrfs.Key
	DevExt btrfsitem.DevExtent
}

func (found ScanOneDevResult) AddToLV(ctx context.Context, fs *btrfs.FS, dev *btrfs.Device) {
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
				Flags:      nil,
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
		flags := bg.Flags
		mapping := btrfsvol.Mapping{
			LAddr: bg.LAddr,
			PAddr: btrfsvol.QualifiedPhysicalAddr{
				Dev:  otherPAddr.Dev,
				Addr: otherPAddr.Addr.Add(-offsetWithinChunk),
			},
			Size:       bg.Size,
			SizeLocked: true,
			Flags:      &flags,
		}
		if err := fs.LV.AddMapping(mapping); err != nil {
			dlog.Errorf(ctx, "... dev[%q] error: adding flags from blockgroup: %v",
				dev.Name(), err)
		}
	}
}

func ScanOneDev(ctx context.Context, dev *btrfs.Device, superblock btrfs.Superblock) (ScanOneDevResult, error) {
	result := ScanOneDevResult{
		FoundNodes: make(map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr),
	}

	devSize, _ := dev.Size()
	lastProgress := -1

	err := btrfsutil.ScanForNodes(ctx, dev, superblock, func(nodeRef *diskio.Ref[btrfsvol.PhysicalAddr, btrfs.Node], err error) {
		if err != nil {
			dlog.Infof(ctx, "... dev[%q] error: %v", dev.Name(), err)
			return
		}
		result.FoundNodes[nodeRef.Data.Head.Addr] = append(result.FoundNodes[nodeRef.Data.Head.Addr], nodeRef.Addr)
		for i, item := range nodeRef.Data.BodyLeaf {
			switch item.Key.ItemType {
			case btrfsitem.CHUNK_ITEM_KEY:
				chunk, ok := item.Body.(btrfsitem.Chunk)
				if !ok {
					dlog.Errorf(ctx, "... dev[%q] node@%v: item %v: error: type is CHUNK_ITEM_KEY, but struct is %T",
						dev.Name(), nodeRef.Addr, i, item.Body)
					continue
				}
				//dlog.Tracef(ctx, "... dev[%q] node@%v: item %v: found chunk",
				//	dev.Name(), nodeRef.Addr, i)
				result.FoundChunks = append(result.FoundChunks, btrfs.SysChunk{
					Key:   item.Key,
					Chunk: chunk,
				})
			case btrfsitem.BLOCK_GROUP_ITEM_KEY:
				bg, ok := item.Body.(btrfsitem.BlockGroup)
				if !ok {
					dlog.Errorf(ctx, "... dev[%q] node@%v: item %v: error: type is BLOCK_GROUP_ITEM_KEY, but struct is %T",
						dev.Name(), nodeRef.Addr, i, item.Body)
					continue
				}
				//dlog.Tracef(ctx, "... dev[%q] node@%v: item %v: found block group",
				//	dev.Name(), nodeRef.Addr, i)
				result.FoundBlockGroups = append(result.FoundBlockGroups, SysBlockGroup{
					Key: item.Key,
					BG:  bg,
				})
			case btrfsitem.DEV_EXTENT_KEY:
				devext, ok := item.Body.(btrfsitem.DevExtent)
				if !ok {
					dlog.Errorf(ctx, "... dev[%q] node@%v: item %v: error: type is DEV_EXTENT_KEY, but struct is %T",
						dev.Name(), nodeRef.Addr, i, item.Body)
					continue
				}
				//dlog.Tracef(ctx, "... dev[%q] node@%v: item %v: found dev extent",
				//	dev.Name(), nodeRef.Addr, i)
				result.FoundDevExtents = append(result.FoundDevExtents, SysDevExtent{
					Key:    item.Key,
					DevExt: devext,
				})
			}
		}
	}, func(pos btrfsvol.PhysicalAddr) {
		pct := int(100 * float64(pos) / float64(devSize))
		if pct != lastProgress || pos == devSize {
			dlog.Infof(ctx, "... dev[%q] scanned %v%% (found: %v nodes, %v chunks, %v block groups, %v dev extents)",
				dev.Name(), pct,
				len(result.FoundNodes),
				len(result.FoundChunks),
				len(result.FoundBlockGroups),
				len(result.FoundDevExtents))
			lastProgress = pct
		}
	})

	return result, err
}
