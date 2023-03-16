// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

var sbSize = btrfsvol.PhysicalAddr(binstruct.StaticSize(btrfstree.Superblock{}))

type DeviceScannerFactory[Stats comparable, Result any] func(ctx context.Context, sb btrfstree.Superblock, numBytes btrfsvol.PhysicalAddr, numSectors int) DeviceScanner[Stats, Result]

type DeviceScanner[Stats comparable, Result any] interface {
	ScanStats() Stats
	ScanSector(ctx context.Context, dev *btrfs.Device, paddr btrfsvol.PhysicalAddr) error
	ScanNode(ctx context.Context, addr btrfsvol.PhysicalAddr, node *btrfstree.Node) error
	ScanDone(ctx context.Context) (Result, error)
}

type scanStats[T comparable] struct {
	portion textui.Portion[btrfsvol.PhysicalAddr]
	stats   T
}

func (s scanStats[T]) String() string {
	return textui.Sprintf("scanned %v (%v)",
		s.portion, s.stats)
}

func ScanDevices[Stats comparable, Result any](ctx context.Context, fs *btrfs.FS, newScanner DeviceScannerFactory[Stats, Result]) (map[btrfsvol.DeviceID]Result, error) {
	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
	var mu sync.Mutex
	result := make(map[btrfsvol.DeviceID]Result)
	for id, dev := range fs.LV.PhysicalVolumes() {
		id := id
		dev := dev
		grp.Go(fmt.Sprintf("dev-%d", id), func(ctx context.Context) error {
			devResult, err := ScanOneDevice[Stats, Result](ctx, dev, newScanner)
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

func ScanOneDevice[Stats comparable, Result any](ctx context.Context, dev *btrfs.Device, newScanner DeviceScannerFactory[Stats, Result]) (Result, error) {
	ctx = dlog.WithField(ctx, "scandevices.dev", dev.Name())

	sb, err := dev.Superblock()
	if err != nil {
		var zero Result
		return zero, err
	}
	numBytes := dev.Size()
	if sb.NodeSize < sb.SectorSize {
		var zero Result
		return zero, fmt.Errorf("node_size(%v) < sector_size(%v)",
			sb.NodeSize, sb.SectorSize)
	}
	if sb.SectorSize != btrfssum.BlockSize {
		// TODO: probably handle this?
		var zero Result
		return zero, fmt.Errorf("sector_size(%v) != btrfssum.BlockSize",
			sb.SectorSize)
	}
	numSectors := int(numBytes / btrfssum.BlockSize)

	scanner := newScanner(ctx, *sb, numBytes, numSectors)

	progressWriter := textui.NewProgress[scanStats[Stats]](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second))
	var stats scanStats[Stats]
	stats.portion.D = numBytes

	var minNextNode btrfsvol.PhysicalAddr
	for i := 0; i < numSectors; i++ {
		if ctx.Err() != nil {
			var zero Result
			return zero, ctx.Err()
		}
		pos := btrfsvol.PhysicalAddr(i * btrfssum.BlockSize)
		stats.portion.N = pos
		stats.stats = scanner.ScanStats()
		progressWriter.Set(stats)

		if err := scanner.ScanSector(ctx, dev, pos); err != nil {
			var zero Result
			return zero, err
		}

		checkForNode := pos >= minNextNode && pos+btrfsvol.PhysicalAddr(sb.NodeSize) <= numBytes
		if checkForNode {
			for _, sbAddr := range btrfs.SuperblockAddrs {
				if sbAddr <= pos && pos < sbAddr+sbSize {
					checkForNode = false
					break
				}
			}
		}

		if checkForNode {
			node, err := btrfstree.ReadNode[btrfsvol.PhysicalAddr](dev, *sb, pos, btrfstree.NodeExpectations{})
			if err != nil {
				if !errors.Is(err, btrfstree.ErrNotANode) {
					dlog.Errorf(ctx, "error: %v", err)
				}
			} else {
				if err := scanner.ScanNode(ctx, pos, node); err != nil {
					var zero Result
					return zero, err
				}
				minNextNode = pos + btrfsvol.PhysicalAddr(sb.NodeSize)
			}
			node.Free()
		}
	}

	stats.portion.N = numBytes
	stats.stats = scanner.ScanStats()
	progressWriter.Set(stats)
	progressWriter.Done()

	return scanner.ScanDone(ctx)
}
