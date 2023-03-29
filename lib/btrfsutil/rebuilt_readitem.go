// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"

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

func (ts *RebuiltForrest) readItem(ctx context.Context, ptr ItemPtr) btrfsitem.Item {
	graphInfo, ok := ts.graph.Nodes[ptr.Node]
	if !ok {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltForrest.readItem called for node@%v not mentioned in the in-memory graph", ptr.Node))
	}
	if graphInfo.Level != 0 {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltForrest.readItem called for non-leaf node@%v", ptr.Node))
	}
	if ptr.Slot < 0 {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltForrest.readItem called for negative item slot: %v", ptr.Slot))
	}

	node, err := ts.file.AcquireNode(ctx, ptr.Node, btrfstree.NodeExpectations{
		LAddr:      containers.OptionalValue(ptr.Node),
		Level:      containers.OptionalValue(graphInfo.Level),
		Generation: containers.OptionalValue(graphInfo.Generation),
		Owner: func(treeID btrfsprim.ObjID, gen btrfsprim.Generation) error {
			if treeID != graphInfo.Owner || gen != graphInfo.Generation {
				return fmt.Errorf("expected owner=%v generation=%v but claims to have owner=%v generation=%v",
					graphInfo.Owner, graphInfo.Generation,
					treeID, gen)
			}
			return nil
		},
		MinItem: containers.OptionalValue(graphInfo.MinItem()),
		MaxItem: containers.OptionalValue(graphInfo.MaxItem()),
	})
	defer ts.file.ReleaseNode(node)
	if err != nil {
		panic(fmt.Errorf("should not happen: i/o error: %w", err))
	}

	items := node.BodyLeaf

	if ptr.Slot >= len(items) {
		panic(fmt.Errorf("should not happen: btrfsutil.RebuiltForrest.readItem called for out-of-bounds item slot: slot=%v len=%v",
			ptr.Slot, len(items)))
	}
	return items[ptr.Slot].Body.CloneItem()
}
