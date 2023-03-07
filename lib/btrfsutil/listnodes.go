// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type nodeScanner struct {
	nodes containers.Set[btrfsvol.LogicalAddr]
}

type nodeStats struct {
	numNodes int
}

func (s nodeStats) String() string {
	return textui.Sprintf("found: %d nodes", s.numNodes)
}

var _ DeviceScanner[nodeStats, containers.Set[btrfsvol.LogicalAddr]] = (*nodeScanner)(nil)

func newNodeScanner(ctx context.Context, sb btrfstree.Superblock, numBytes btrfsvol.PhysicalAddr, numSectors int) DeviceScanner[nodeStats, containers.Set[btrfsvol.LogicalAddr]] {
	s := new(nodeScanner)
	s.nodes = make(containers.Set[btrfsvol.LogicalAddr])
	return s
}

func (s *nodeScanner) ScanStats() nodeStats {
	return nodeStats{numNodes: len(s.nodes)}
}

func (*nodeScanner) ScanSector(ctx context.Context, dev *btrfs.Device, paddr btrfsvol.PhysicalAddr) error {
	return nil
}

func (s *nodeScanner) ScanNode(ctx context.Context, nodeRef *diskio.Ref[btrfsvol.PhysicalAddr, btrfstree.Node]) error {
	s.nodes.Insert(nodeRef.Data.Head.Addr)
	return nil
}

func (s *nodeScanner) ScanDone(ctx context.Context) (containers.Set[btrfsvol.LogicalAddr], error) {
	return s.nodes, nil
}

func ListNodes(ctx context.Context, fs *btrfs.FS) ([]btrfsvol.LogicalAddr, error) {
	perDev, err := ScanDevices[nodeStats, containers.Set[btrfsvol.LogicalAddr]](ctx, fs, newNodeScanner)
	if err != nil {
		return nil, err
	}
	set := make(containers.Set[btrfsvol.LogicalAddr])
	for _, devSet := range perDev {
		set.InsertFrom(devSet)
	}
	return maps.SortedKeys(set), nil
}
