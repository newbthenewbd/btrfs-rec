// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type ItemPtr struct {
	Node btrfsvol.LogicalAddr
	Slot int
}

func (ptr ItemPtr) String() string {
	return fmt.Sprintf("node@%v[%v]", ptr.Node, ptr.Slot)
}

func (ts *RebuiltForrest) readNode(ctx context.Context, laddr btrfsvol.LogicalAddr) *btrfstree.Node {
	if cached, ok := ts.nodes.Load(laddr); ok {
		dlog.Tracef(ctx, "cache-hit node@%v", laddr)
		return cached
	}

	graphInfo, ok := ts.graph.Nodes[laddr]
	if !ok {
		panic(fmt.Errorf("should not happen: node@%v is not mentioned in the in-memory graph", laddr))
	}

	dlog.Debugf(ctx, "cache-miss node@%v, reading...", laddr)
	node, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](ts.file, ts.sb, laddr, btrfstree.NodeExpectations{
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

	ts.nodes.Store(laddr, node)

	return node
}

func (ts *RebuiltForrest) readItem(ctx context.Context, ptr ItemPtr) btrfsitem.Item {
	ts.nodesMu.Lock()
	defer ts.nodesMu.Unlock()
	if ts.graph.Nodes[ptr.Node].Level != 0 {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltForrest.readItem called for non-leaf node@%v", ptr.Node))
	}
	if ptr.Slot < 0 {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltForrest.readItem called for negative item slot: %v", ptr.Slot))
	}
	items := ts.readNode(ctx, ptr.Node).BodyLeaf
	if ptr.Slot >= len(items) {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltForrest.readItem called for out-of-bounds item slot: slot=%v len=%v",
			ptr.Slot, len(items)))
	}
	return items[ptr.Slot].Body.CloneItem()
}
