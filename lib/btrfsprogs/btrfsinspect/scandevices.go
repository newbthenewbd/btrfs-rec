// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsinspect

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
)

type ScanDevicesResult map[btrfsvol.DeviceID]ScanOneDeviceResult

func ScanDevices(ctx context.Context, fs *btrfs.FS) (ScanDevicesResult, error) {
	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
	var mu sync.Mutex
	result := make(map[btrfsvol.DeviceID]ScanOneDeviceResult)
	for id, dev := range fs.LV.PhysicalVolumes() {
		id := id
		dev := dev
		grp.Go(dev.Name(), func(ctx context.Context) error {
			sb, err := dev.Superblock()
			if err != nil {
				return err
			}
			devResult, err := ScanOneDevice(ctx, dev, *sb)
			if err != nil {
				return err
			}
			mu.Lock()
			result[id] = devResult
			mu.Unlock()
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return nil, err
	}
	return result, nil
}

type ScanOneDeviceResult struct {
	Checksums        SumRun[btrfsvol.PhysicalAddr]
	FoundNodes       map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr
	FoundChunks      []btrfs.SysChunk
	FoundBlockGroups []SysBlockGroup
	FoundDevExtents  []SysDevExtent
	FoundExtentCSums []SysExtentCSum
}

type SysBlockGroup struct {
	Key btrfs.Key
	BG  btrfsitem.BlockGroup
}

type SysDevExtent struct {
	Key    btrfs.Key
	DevExt btrfsitem.DevExtent
}

type SysExtentCSum struct {
	Key        btrfs.Key
	Generation btrfs.Generation
	Sums       btrfsitem.ExtentCSum
}

// ScanOneDevice mostly mimics btrfs-progs
// cmds/rescue-chunk-recover.c:scan_one_device().
func ScanOneDevice(ctx context.Context, dev *btrfs.Device, sb btrfs.Superblock) (ScanOneDeviceResult, error) {
	result := ScanOneDeviceResult{
		FoundNodes: make(map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr),
	}

	sbSize := btrfsvol.PhysicalAddr(binstruct.StaticSize(btrfs.Superblock{}))

	devSize := dev.Size()
	if sb.NodeSize < sb.SectorSize {
		return result, fmt.Errorf("node_size(%v) < sector_size(%v)",
			sb.NodeSize, sb.SectorSize)
	}
	if sb.SectorSize != btrfsitem.CSumBlockSize {
		// TODO: probably handle this?
		return result, fmt.Errorf("sector_size(%v) != btrfsitem.CSumBlockSize",
			sb.SectorSize)
	}
	alg := sb.ChecksumType
	csumSize := alg.Size()
	numSums := int(devSize / btrfsitem.CSumBlockSize)
	sums := make([]byte, numSums*csumSize)

	lastProgress := -1
	progress := func(pos btrfsvol.PhysicalAddr) {
		pct := int(100 * float64(pos) / float64(devSize))
		if pct != lastProgress || pos == devSize {
			dlog.Infof(ctx, "... dev[%q] scanned %v%% (found: %v nodes, %v chunks, %v block groups, %v dev extents, %v sum items)",
				dev.Name(), pct,
				len(result.FoundNodes),
				len(result.FoundChunks),
				len(result.FoundBlockGroups),
				len(result.FoundDevExtents),
				len(result.FoundExtentCSums))
			lastProgress = pct
		}
	}

	var minNextNode btrfsvol.PhysicalAddr
	for i := 0; i < numSums; i++ {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		pos := btrfsvol.PhysicalAddr(i * btrfsitem.CSumBlockSize)
		progress(pos)

		sum, err := btrfsutil.ChecksumPhysical(dev, alg, pos)
		if err != nil {
			return result, err
		}
		copy(sums[i*csumSize:], sum[:csumSize])

		checkForNode := pos >= minNextNode
		if checkForNode {
			for _, sbAddr := range btrfs.SuperblockAddrs {
				if sbAddr <= pos && pos < sbAddr+sbSize {
					checkForNode = false
					break
				}
			}
		}

		if checkForNode {
			nodeRef, err := btrfs.ReadNode[btrfsvol.PhysicalAddr](dev, sb, pos, nil)
			if err != nil {
				if !errors.Is(err, btrfs.ErrNotANode) {
					dlog.Infof(ctx, "... dev[%q] error: %v", dev.Name(), err)
				}
			} else {
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
					case btrfsitem.EXTENT_CSUM_KEY:
						sums, ok := item.Body.(btrfsitem.ExtentCSum)
						if !ok {
							dlog.Errorf(ctx, "... dev[%q] node@%v: item %v: error: type is EXTENT_CSUM_OBJECTID, but struct is %T",
								dev.Name(), nodeRef.Addr, i, item.Body)
							continue
						}
						//dlog.Tracef(ctx, "... dev[%q] node@%v: item %v: found csums",
						//	dev.Name(), nodeRef.Addr, i)
						result.FoundExtentCSums = append(result.FoundExtentCSums, SysExtentCSum{
							Key:        item.Key,
							Generation: nodeRef.Data.Head.Generation,
							Sums:       sums,
						})
					}
				}
				minNextNode = pos + btrfsvol.PhysicalAddr(sb.NodeSize)
			}
		}
	}
	progress(devSize)

	return result, nil
}
