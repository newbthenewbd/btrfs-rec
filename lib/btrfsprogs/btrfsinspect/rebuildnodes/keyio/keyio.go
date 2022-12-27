// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package keyio

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type ItemPtr struct {
	Node btrfsvol.LogicalAddr
	Idx  int
}

func (ptr ItemPtr) String() string {
	return fmt.Sprintf("node@%v[%v]", ptr.Node, ptr.Idx)
}

type SizeAndErr struct {
	Size uint64
	Err  error
}

type Handle struct {
	rawFile diskio.File[btrfsvol.LogicalAddr]
	sb      btrfstree.Superblock
	graph   graph.Graph

	Names map[ItemPtr][]byte
	Sizes map[ItemPtr]SizeAndErr

	cache *containers.LRUCache[btrfsvol.LogicalAddr, *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]]
}

func NewHandle(file diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock) *Handle {
	return &Handle{
		rawFile: file,
		sb:      sb,

		Names: make(map[ItemPtr][]byte),
		Sizes: make(map[ItemPtr]SizeAndErr),

		cache: containers.NewLRUCache[btrfsvol.LogicalAddr, *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]](textui.Tunable(8)),
	}
}

func (o *Handle) InsertNode(nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) {
	for i, item := range nodeRef.Data.BodyLeaf {
		ptr := ItemPtr{
			Node: nodeRef.Addr,
			Idx:  i,
		}
		switch itemBody := item.Body.(type) {
		case btrfsitem.DirEntry:
			if item.Key.ItemType == btrfsprim.DIR_INDEX_KEY {
				o.Names[ptr] = append([]byte(nil), itemBody.Name...)
			}
		case btrfsitem.ExtentCSum:
			o.Sizes[ptr] = SizeAndErr{
				Size: uint64(itemBody.Size()),
				Err:  nil,
			}
		case btrfsitem.FileExtent:
			size, err := itemBody.Size()
			o.Sizes[ptr] = SizeAndErr{
				Size: uint64(size),
				Err:  err,
			}
		case btrfsitem.Error:
			switch item.Key.ItemType {
			case btrfsprim.EXTENT_CSUM_KEY, btrfsprim.EXTENT_DATA_KEY:
				o.Sizes[ptr] = SizeAndErr{
					Err: fmt.Errorf("error decoding item: ptr=%v (tree=%v key=%v): %w",
						ptr, nodeRef.Data.Head.Owner, item.Key, itemBody.Err),
				}
			}
		}
	}
}

func (o *Handle) SetGraph(graph graph.Graph) {
	o.graph = graph
}

func (o *Handle) readNode(ctx context.Context, laddr btrfsvol.LogicalAddr) *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node] {
	if cached, ok := o.cache.Get(laddr); ok {
		dlog.Tracef(ctx, "cache-hit node@%v", laddr)
		return cached
	}

	graphInfo, ok := o.graph.Nodes[laddr]
	if !ok {
		panic(fmt.Errorf("should not happen: node@%v is not mentioned in the in-memory graph", laddr))
	}

	dlog.Infof(ctx, "cache-miss node@%v, reading...", laddr)
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

func (o *Handle) ReadItem(ctx context.Context, ptr ItemPtr) (item btrfsitem.Item, ok bool) {
	if o.graph.Nodes[ptr.Node].Level != 0 || ptr.Idx < 0 {
		return nil, false
	}
	items := o.readNode(ctx, ptr.Node).Data.BodyLeaf
	if ptr.Idx >= len(items) {
		return nil, false
	}
	return items[ptr.Idx].Body, true
}
