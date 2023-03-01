// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"
	"sync"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type ItemPtr struct {
	Node btrfsvol.LogicalAddr
	Slot int
}

func (ptr ItemPtr) String() string {
	return fmt.Sprintf("node@%v[%v]", ptr.Node, ptr.Slot)
}

type KeyIO struct {
	rawFile diskio.File[btrfsvol.LogicalAddr]
	sb      btrfstree.Superblock
	graph   Graph

	mu    sync.Mutex
	cache containers.ARCache[btrfsvol.LogicalAddr, *btrfstree.Node]
}

func NewKeyIO(file diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock, graph Graph) *KeyIO {
	return &KeyIO{
		rawFile: file,
		sb:      sb,
		graph:   graph,

		cache: containers.ARCache[btrfsvol.LogicalAddr, *btrfstree.Node]{
			MaxLen: textui.Tunable(8),
			OnRemove: func(_ btrfsvol.LogicalAddr, node *btrfstree.Node) {
				node.Free()
			},
		},
	}
}

func (o *KeyIO) readNode(ctx context.Context, laddr btrfsvol.LogicalAddr) *btrfstree.Node {
	if cached, ok := o.cache.Load(laddr); ok {
		dlog.Tracef(ctx, "cache-hit node@%v", laddr)
		return cached
	}

	graphInfo, ok := o.graph.Nodes[laddr]
	if !ok {
		panic(fmt.Errorf("should not happen: node@%v is not mentioned in the in-memory graph", laddr))
	}

	dlog.Debugf(ctx, "cache-miss node@%v, reading...", laddr)
	node, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](o.rawFile, o.sb, laddr, btrfstree.NodeExpectations{
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
		MinItem: containers.Optional[btrfsprim.Key]{OK: true, Val: graphInfo.MinItem()},
		MaxItem: containers.Optional[btrfsprim.Key]{OK: true, Val: graphInfo.MaxItem()},
	})
	if err != nil {
		panic(fmt.Errorf("should not happen: i/o error: %w", err))
	}

	o.cache.Store(laddr, node)

	return node
}

func (o *KeyIO) ReadItem(ctx context.Context, ptr ItemPtr) btrfsitem.Item {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.graph.Nodes[ptr.Node].Level != 0 {
		panic(fmt.Errorf("should not happen: btrfsutil.KeyIO.ReadItem called for non-leaf node@%v", ptr.Node))
	}
	if ptr.Slot < 0 {
		panic(fmt.Errorf("should not happen: btrfsutil.KeyIO.ReadItem called for negative item slot: %v", ptr.Slot))
	}
	items := o.readNode(ctx, ptr.Node).BodyLeaf
	if ptr.Slot >= len(items) {
		panic(fmt.Errorf("should not happen: btrfsutil.KeyIO.ReadItem called for out-of-bounds item slot: slot=%v len=%v",
			ptr.Slot, len(items)))
	}
	return items[ptr.Slot].Body.CloneItem()
}
