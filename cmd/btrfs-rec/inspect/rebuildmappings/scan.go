// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"fmt"
	"strings"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

// Result types ////////////////////////////////////////////////////////////////

type ScanDevicesResult = map[btrfsvol.DeviceID]ScanOneDeviceResult

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

// Convenience functions for those types ///////////////////////////////////////

func ScanDevices(ctx context.Context, fs *btrfs.FS) (ScanDevicesResult, error) {
	return btrfsutil.ScanDevices[scanStats, ScanOneDeviceResult](ctx, fs, newDeviceScanner)
}

// ScanOneDevice mostly mimics btrfs-progs
// cmds/rescue-chunk-recover.c:scan_one_device().
func ScanOneDevice(ctx context.Context, dev *btrfs.Device) (ScanOneDeviceResult, error) {
	return btrfsutil.ScanOneDevice[scanStats, ScanOneDeviceResult](ctx, dev, newDeviceScanner)
}

// scanner implementation //////////////////////////////////////////////////////

type deviceScanner struct {
	alg    btrfssum.CSumType
	sums   strings.Builder
	result ScanOneDeviceResult
}

type scanStats struct {
	NumFoundNodes       int
	NumFoundChunks      int
	NumFoundBlockGroups int
	NumFoundDevExtents  int
	NumFoundExtentCSums int
}

func (s scanStats) String() string {
	return textui.Sprintf("found: %v nodes, %v chunks, %v block groups, %v dev extents, %v sum items",
		s.NumFoundNodes,
		s.NumFoundChunks,
		s.NumFoundBlockGroups,
		s.NumFoundDevExtents,
		s.NumFoundExtentCSums)
}

func (scanner *deviceScanner) ScanStats() scanStats {
	return scanStats{
		NumFoundNodes:       len(scanner.result.FoundNodes),
		NumFoundChunks:      len(scanner.result.FoundChunks),
		NumFoundBlockGroups: len(scanner.result.FoundBlockGroups),
		NumFoundDevExtents:  len(scanner.result.FoundDevExtents),
		NumFoundExtentCSums: len(scanner.result.FoundExtentCSums),
	}
}

func newDeviceScanner(ctx context.Context, sb btrfstree.Superblock, numBytes btrfsvol.PhysicalAddr, numSectors int) btrfsutil.DeviceScanner[scanStats, ScanOneDeviceResult] {
	scanner := new(deviceScanner)
	scanner.alg = sb.ChecksumType
	scanner.result.FoundNodes = make(map[btrfsvol.LogicalAddr][]btrfsvol.PhysicalAddr)
	scanner.result.Checksums.ChecksumSize = scanner.alg.Size()
	scanner.sums.Grow(scanner.result.Checksums.ChecksumSize * numSectors)
	return scanner
}

func (scanner *deviceScanner) ScanSector(ctx context.Context, dev *btrfs.Device, paddr btrfsvol.PhysicalAddr) error {
	sum, err := btrfs.ChecksumPhysical(dev, scanner.alg, paddr)
	if err != nil {
		return err
	}
	scanner.sums.Write(sum[:scanner.result.Checksums.ChecksumSize])
	return nil
}

func (scanner *deviceScanner) ScanNode(ctx context.Context, nodeRef *diskio.Ref[btrfsvol.PhysicalAddr, btrfstree.Node]) error {
	scanner.result.FoundNodes[nodeRef.Data.Head.Addr] = append(scanner.result.FoundNodes[nodeRef.Data.Head.Addr], nodeRef.Addr)
	for i, item := range nodeRef.Data.BodyLeaf {
		switch item.Key.ItemType {
		case btrfsitem.CHUNK_ITEM_KEY:
			switch itemBody := item.Body.(type) {
			case *btrfsitem.Chunk:
				dlog.Tracef(ctx, "node@%v: item %v: found chunk",
					nodeRef.Addr, i)
				scanner.result.FoundChunks = append(scanner.result.FoundChunks, btrfstree.SysChunk{
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
				scanner.result.FoundBlockGroups = append(scanner.result.FoundBlockGroups, SysBlockGroup{
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
				scanner.result.FoundDevExtents = append(scanner.result.FoundDevExtents, SysDevExtent{
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
				scanner.result.FoundExtentCSums = append(scanner.result.FoundExtentCSums, SysExtentCSum{
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
	return nil
}

func (scanner *deviceScanner) ScanDone(ctx context.Context) (ScanOneDeviceResult, error) {
	scanner.result.Checksums.Sums = btrfssum.ShortSum(scanner.sums.String())
	return scanner.result, nil
}
