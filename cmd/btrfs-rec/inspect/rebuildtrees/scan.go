// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildtrees

import (
	"context"
	"fmt"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type SizeAndErr struct {
	Size uint64
	Err  error
}

type FlagsAndErr struct {
	NoDataSum bool
	Err       error
}

type ScanDevicesResult struct {
	Superblock btrfstree.Superblock

	Graph btrfsutil.Graph

	Flags map[btrfsutil.ItemPtr]FlagsAndErr // INODE_ITEM
	Names map[btrfsutil.ItemPtr][]byte      // DIR_INDEX
	Sizes map[btrfsutil.ItemPtr]SizeAndErr  // EXTENT_CSUM and EXTENT_DATA
}

func ScanDevices(ctx context.Context, fs *btrfs.FS, nodeList []btrfsvol.LogicalAddr) (ScanDevicesResult, error) {
	dlog.Info(ctx, "Reading superblock...")
	sb, err := fs.Superblock()
	if err != nil {
		return ScanDevicesResult{}, err
	}

	dlog.Infof(ctx, "Reading node data from FS...")

	var stats textui.Portion[int]
	stats.D = len(nodeList)
	progressWriter := textui.NewProgress[textui.Portion[int]](
		dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.read.substep", "read-nodes"),
		dlog.LogLevelInfo, textui.Tunable(1*time.Second))

	ret := ScanDevicesResult{
		Superblock: *sb,

		Graph: btrfsutil.NewGraph(ctx, *sb),

		Flags: make(map[btrfsutil.ItemPtr]FlagsAndErr),
		Names: make(map[btrfsutil.ItemPtr][]byte),
		Sizes: make(map[btrfsutil.ItemPtr]SizeAndErr),
	}

	progressWriter.Set(stats)
	for _, laddr := range nodeList {
		if err := ctx.Err(); err != nil {
			return ScanDevicesResult{}, err
		}
		node, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
			LAddr: containers.OptionalValue(laddr),
		})
		if err != nil {
			node.Free()
			return ScanDevicesResult{}, err
		}

		ret.insertNode(node)

		node.Free()

		stats.N++
		progressWriter.Set(stats)
	}
	if stats.N != stats.D {
		panic("should not happen")
	}
	progressWriter.Done()
	dlog.Info(ctx, "... done reading node data")

	ctx = dlog.WithField(ctx, "btrfs.inspect.rebuild-trees.read.substep", "check")
	if err := ret.Graph.FinalCheck(ctx, fs, *sb); err != nil {
		return ScanDevicesResult{}, err
	}

	return ret, nil
}

func (o *ScanDevicesResult) insertNode(node *btrfstree.Node) {
	o.Graph.InsertNode(node)
	for i, item := range node.BodyLeaf {
		ptr := btrfsutil.ItemPtr{
			Node: node.Head.Addr,
			Slot: i,
		}
		switch itemBody := item.Body.(type) {
		case *btrfsitem.Inode:
			o.Flags[ptr] = FlagsAndErr{
				NoDataSum: itemBody.Flags.Has(btrfsitem.INODE_NODATASUM),
				Err:       nil,
			}
		case *btrfsitem.DirEntry:
			if item.Key.ItemType == btrfsprim.DIR_INDEX_KEY {
				o.Names[ptr] = append([]byte(nil), itemBody.Name...)
			}
		case *btrfsitem.ExtentCSum:
			o.Sizes[ptr] = SizeAndErr{
				Size: uint64(itemBody.Size()),
				Err:  nil,
			}
		case *btrfsitem.FileExtent:
			size, err := itemBody.Size()
			o.Sizes[ptr] = SizeAndErr{
				Size: uint64(size),
				Err:  err,
			}
		case *btrfsitem.Error:
			switch item.Key.ItemType {
			case btrfsprim.INODE_ITEM_KEY:
				o.Flags[ptr] = FlagsAndErr{
					Err: fmt.Errorf("error decoding item: ptr=%v (tree=%v key=%v): %w",
						ptr, node.Head.Owner, item.Key, itemBody.Err),
				}
			case btrfsprim.EXTENT_CSUM_KEY, btrfsprim.EXTENT_DATA_KEY:
				o.Sizes[ptr] = SizeAndErr{
					Err: fmt.Errorf("error decoding item: ptr=%v (tree=%v key=%v): %w",
						ptr, node.Head.Owner, item.Key, itemBody.Err),
				}
			}
		}
	}
}
