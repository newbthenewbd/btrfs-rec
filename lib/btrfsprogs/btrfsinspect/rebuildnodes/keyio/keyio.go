// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package keyio

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type ItemPtr struct {
	Node btrfsvol.LogicalAddr
	Idx  int
}

func (ptr ItemPtr) String() string {
	return fmt.Sprintf("node@%v[%v]", ptr.Node, ptr.Idx)
}

type Handle struct {
	rawFile diskio.File[btrfsvol.LogicalAddr]
	sb      btrfstree.Superblock
	graph   graph.Graph

	cache *containers.LRUCache[btrfsvol.LogicalAddr, *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]]
}

func NewHandle(file diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock, graph graph.Graph) *Handle {
	return &Handle{
		rawFile: file,
		sb:      sb,
		graph:   graph,

		cache: containers.NewLRUCache[btrfsvol.LogicalAddr, *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]](8),
	}
}

func (o *Handle) ReadNode(laddr btrfsvol.LogicalAddr) *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node] {
	if cached, ok := o.cache.Get(laddr); ok {
		return cached
	}

	graphInfo, ok := o.graph.Nodes[laddr]
	if !ok {
		panic(fmt.Errorf("should not happen: node@%v is not mentioned in the in-memory graph", laddr))
	}

	ref, err := btrfstree.ReadNode(o.rawFile, o.sb, laddr, btrfstree.NodeExpectations{
		LAddr:      containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
		Level:      containers.Optional[uint8]{OK: true, Val: graphInfo.Level},
		Generation: containers.Optional[btrfsprim.Generation]{OK: true, Val: graphInfo.Generation},
		Owner: func(treeID btrfsprim.ObjID) error {
			if treeID != graphInfo.Owner {
				return fmt.Errorf("expected owner=%v but claims to have owner=%v",
					graphInfo.Owner, treeID)
			}
			return nil
		},
		MinItem: containers.Optional[btrfsprim.Key]{OK: true, Val: graphInfo.MinItem},
		MaxItem: containers.Optional[btrfsprim.Key]{OK: true, Val: graphInfo.MaxItem},
	})
	if err != nil {
		panic(fmt.Errorf("should not happen: i/o error: %w", err))
	}

	o.cache.Add(laddr, ref)

	return ref
}

func (o *Handle) ReadItem(ptr ItemPtr) (item btrfsitem.Item, ok bool) {
	if o.graph.Nodes[ptr.Node].Level != 0 || ptr.Idx < 0 {
		return nil, false
	}
	items := o.ReadNode(ptr.Node).Data.BodyLeaf
	if ptr.Idx >= len(items) {
		return nil, false
	}
	return items[ptr.Idx].Body, true
}
