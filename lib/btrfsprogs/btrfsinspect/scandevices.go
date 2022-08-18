// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsinspect

import (
	"context"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type ScanOneDeviceResult struct {
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

// ScanOneDevice mostly mimics btrfs-progs
// cmds/rescue-chunk-recover.c:scan_one_device().
func ScanOneDevice(ctx context.Context, dev *btrfs.Device, superblock btrfs.Superblock) (ScanOneDeviceResult, error) {
	result := ScanOneDeviceResult{
		FoundNodes: make(map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr),
	}

	devSize := dev.Size()
	lastProgress := -1

	err := ScanForNodes(ctx, dev, superblock, func(nodeRef *diskio.Ref[btrfsvol.PhysicalAddr, btrfs.Node], err error) {
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
