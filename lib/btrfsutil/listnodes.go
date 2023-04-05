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
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type nodeLister struct {
	nodes containers.Set[btrfsvol.LogicalAddr]
}

type nodeListStats struct {
	numNodes int
}

func (s nodeListStats) String() string {
	return textui.Sprintf("found: %d nodes", s.numNodes)
}

var _ DeviceScanner[nodeListStats, containers.Set[btrfsvol.LogicalAddr]] = (*nodeLister)(nil)

func newNodeLister(context.Context, btrfstree.Superblock, btrfsvol.PhysicalAddr, int) DeviceScanner[nodeListStats, containers.Set[btrfsvol.LogicalAddr]] {
	s := new(nodeLister)
	s.nodes = make(containers.Set[btrfsvol.LogicalAddr])
	return s
}

func (s *nodeLister) ScanStats() nodeListStats {
	return nodeListStats{numNodes: len(s.nodes)}
}

func (*nodeLister) ScanSector(context.Context, *btrfs.Device, btrfsvol.PhysicalAddr) error {
	return nil
}

func (s *nodeLister) ScanNode(_ context.Context, _ btrfsvol.PhysicalAddr, node *btrfstree.Node) error {
	s.nodes.Insert(node.Head.Addr)
	return nil
}

func (s *nodeLister) ScanDone(_ context.Context) (containers.Set[btrfsvol.LogicalAddr], error) {
	return s.nodes, nil
}

func ListNodes(ctx context.Context, fs *btrfs.FS) ([]btrfsvol.LogicalAddr, error) {
	perDev, err := ScanDevices[nodeListStats, containers.Set[btrfsvol.LogicalAddr]](ctx, fs, newNodeLister)
	if err != nil {
		return nil, err
	}
	set := make(containers.Set[btrfsvol.LogicalAddr])
	for _, devSet := range perDev {
		set.InsertFrom(devSet)
	}
	return maps.SortedKeys(set), nil
}
