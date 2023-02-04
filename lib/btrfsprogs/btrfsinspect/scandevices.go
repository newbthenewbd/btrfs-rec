// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsinspect

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type ScanDevicesResult map[btrfsvol.DeviceID]ScanOneDeviceResult

func ScanDevices(ctx context.Context, fs *btrfs.FS) (ScanDevicesResult, error) {
	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
	var mu sync.Mutex
	result := make(map[btrfsvol.DeviceID]ScanOneDeviceResult)
	for id, dev := range fs.LV.PhysicalVolumes() {
		id := id
		dev := dev
		grp.Go(fmt.Sprintf("dev-%d", id), func(ctx context.Context) error {
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
	Checksums        btrfssum.SumRun[btrfsvol.PhysicalAddr]
	FoundNodes       map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr
	FoundChunks      []btrfstree.SysChunk
	FoundBlockGroups []SysBlockGroup
	FoundDevExtents  []SysDevExtent
	FoundExtentCSums []SysExtentCSum
}

type SysBlockGroup struct {
	Key btrfsprim.Key
	BG  btrfsitem.BlockGroup
}

type SysDevExtent struct {
	Key    btrfsprim.Key
	DevExt btrfsitem.DevExtent
}

type SysExtentCSum struct {
	Generation btrfsprim.Generation
	Sums       btrfsitem.ExtentCSum
}

// Compare implements containers.Ordered.
func (a SysExtentCSum) Compare(b SysExtentCSum) int {
	return containers.NativeCompare(a.Sums.Addr, b.Sums.Addr)
}

type scanStats struct {
	textui.Portion[btrfsvol.PhysicalAddr]

	NumFoundNodes       int
	NumFoundChunks      int
	NumFoundBlockGroups int
	NumFoundDevExtents  int
	NumFoundExtentCSums int
}

func (s scanStats) String() string {
	return textui.Sprintf("scanned %v (found: %v nodes, %v chunks, %v block groups, %v dev extents, %v sum items)",
		s.Portion,
		s.NumFoundNodes,
		s.NumFoundChunks,
		s.NumFoundBlockGroups,
		s.NumFoundDevExtents,
		s.NumFoundExtentCSums)
}

var sbSize = btrfsvol.PhysicalAddr(binstruct.StaticSize(btrfstree.Superblock{}))

// ScanOneDevice mostly mimics btrfs-progs
// cmds/rescue-chunk-recover.c:scan_one_device().
func ScanOneDevice(ctx context.Context, dev *btrfs.Device, sb btrfstree.Superblock) (ScanOneDeviceResult, error) {
	ctx = dlog.WithField(ctx, "btrfsinspect.scandevices.dev", dev.Name())

	result := ScanOneDeviceResult{
		FoundNodes: make(map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr),
	}

	devSize := dev.Size()
	if sb.NodeSize < sb.SectorSize {
		return result, fmt.Errorf("node_size(%v) < sector_size(%v)",
			sb.NodeSize, sb.SectorSize)
	}
	if sb.SectorSize != btrfssum.BlockSize {
		// TODO: probably handle this?
		return result, fmt.Errorf("sector_size(%v) != btrfssum.BlockSize",
			sb.SectorSize)
	}
	alg := sb.ChecksumType
	csumSize := alg.Size()
	numSums := int(devSize / btrfssum.BlockSize)
	var sums strings.Builder
	sums.Grow(numSums * csumSize)

	progressWriter := textui.NewProgress[scanStats](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	progress := func(pos btrfsvol.PhysicalAddr) {
		progressWriter.Set(scanStats{
			Portion: textui.Portion[btrfsvol.PhysicalAddr]{
				N: pos,
				D: devSize,
			},
			NumFoundNodes:       len(result.FoundNodes),
			NumFoundChunks:      len(result.FoundChunks),
			NumFoundBlockGroups: len(result.FoundBlockGroups),
			NumFoundDevExtents:  len(result.FoundDevExtents),
			NumFoundExtentCSums: len(result.FoundExtentCSums),
		})
	}

	var minNextNode btrfsvol.PhysicalAddr
	for i := 0; i < numSums; i++ {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		pos := btrfsvol.PhysicalAddr(i * btrfssum.BlockSize)
		progress(pos)

		sum, err := btrfs.ChecksumPhysical(dev, alg, pos)
		if err != nil {
			return result, err
		}
		sums.Write(sum[:csumSize])

		checkForNode := pos >= minNextNode && pos+btrfsvol.PhysicalAddr(sb.NodeSize) <= devSize
		if checkForNode {
			for _, sbAddr := range btrfs.SuperblockAddrs {
				if sbAddr <= pos && pos < sbAddr+sbSize {
					checkForNode = false
					break
				}
			}
		}

		if checkForNode {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.PhysicalAddr](dev, sb, pos, btrfstree.NodeExpectations{})
			if err != nil {
				if !errors.Is(err, btrfstree.ErrNotANode) {
					dlog.Errorf(ctx, "error: %v", err)
				}
			} else {
				result.FoundNodes[nodeRef.Data.Head.Addr] = append(result.FoundNodes[nodeRef.Data.Head.Addr], nodeRef.Addr)
				for i, item := range nodeRef.Data.BodyLeaf {
					switch item.Key.ItemType {
					case btrfsitem.CHUNK_ITEM_KEY:
						switch itemBody := item.Body.(type) {
						case *btrfsitem.Chunk:
							dlog.Tracef(ctx, "node@%v: item %v: found chunk",
								nodeRef.Addr, i)
							result.FoundChunks = append(result.FoundChunks, btrfstree.SysChunk{
								Key:   item.Key,
								Chunk: *itemBody,
							})
						case *btrfsitem.Error:
							dlog.Errorf(ctx, "node@%v: item %v: error: malformed CHUNK_ITEM: %v",
								nodeRef.Addr, i, itemBody.Err)
						default:
							panic(fmt.Errorf("should not happen: CHUNK_ITEM has unexpected item type: %T", itemBody))
						}
					case btrfsitem.BLOCK_GROUP_ITEM_KEY:
						switch itemBody := item.Body.(type) {
						case *btrfsitem.BlockGroup:
							dlog.Tracef(ctx, "node@%v: item %v: found block group",
								nodeRef.Addr, i)
							result.FoundBlockGroups = append(result.FoundBlockGroups, SysBlockGroup{
								Key: item.Key,
								BG:  *itemBody,
							})
						case *btrfsitem.Error:
							dlog.Errorf(ctx, "node@%v: item %v: error: malformed BLOCK_GROUP_ITEM: %v",
								nodeRef.Addr, i, itemBody.Err)
						default:
							panic(fmt.Errorf("should not happen: BLOCK_GROUP_ITEM has unexpected item type: %T", itemBody))
						}
					case btrfsitem.DEV_EXTENT_KEY:
						switch itemBody := item.Body.(type) {
						case *btrfsitem.DevExtent:
							dlog.Tracef(ctx, "node@%v: item %v: found dev extent",
								nodeRef.Addr, i)
							result.FoundDevExtents = append(result.FoundDevExtents, SysDevExtent{
								Key:    item.Key,
								DevExt: *itemBody,
							})
						case *btrfsitem.Error:
							dlog.Errorf(ctx, "node@%v: item %v: error: malformed DEV_EXTENT: %v",
								nodeRef.Addr, i, itemBody.Err)
						default:
							panic(fmt.Errorf("should not happen: DEV_EXTENT has unexpected item type: %T", itemBody))
						}
					case btrfsitem.EXTENT_CSUM_KEY:
						switch itemBody := item.Body.(type) {
						case *btrfsitem.ExtentCSum:
							dlog.Tracef(ctx, "node@%v: item %v: found csums",
								nodeRef.Addr, i)
							result.FoundExtentCSums = append(result.FoundExtentCSums, SysExtentCSum{
								Generation: nodeRef.Data.Head.Generation,
								Sums:       *itemBody,
							})
						case *btrfsitem.Error:
							dlog.Errorf(ctx, "node@%v: item %v: error: malformed is EXTENT_CSUM: %v",
								nodeRef.Addr, i, itemBody.Err)
						default:
							panic(fmt.Errorf("should not happen: EXTENT_CSUM has unexpected item type: %T", itemBody))
						}
					}
				}
				minNextNode = pos + btrfsvol.PhysicalAddr(sb.NodeSize)
			}
		}
	}
	progress(devSize)
	progressWriter.Done()

	result.Checksums = btrfssum.SumRun[btrfsvol.PhysicalAddr]{
		ChecksumSize: csumSize,
		Sums:         btrfssum.ShortSum(sums.String()),
	}

	return result, nil
}
