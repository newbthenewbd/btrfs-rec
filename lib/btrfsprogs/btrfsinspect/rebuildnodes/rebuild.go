// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

type Rebuilder struct {
	inner interface {
		btrfstree.TreeOperator
		Augment(treeID btrfsprim.ObjID, nodeAddr btrfsvol.LogicalAddr) ([]btrfsprim.Key, error)
	}

	orphans      containers.Set[btrfsvol.LogicalAddr]
	leaf2orphans map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr]
	key2leaf     containers.SortedMap[keyAndTree, btrfsvol.LogicalAddr]

	augments map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]
}

func RebuildNodes(ctx context.Context, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) (map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr], error) {
	scanData, err := ScanDevices(ctx, fs, nodeScanResults) // ScanDevices does its own logging
	if err != nil {
		return nil, err
	}

	dlog.Info(ctx, "Reading superblock...")
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	dlog.Info(ctx, "Indexing orphans...")
	orphans, leaf2orphans, key2leaf, err := indexOrphans(fs, *sb, *scanData.nodeGraph)
	if err != nil {
		return nil, err
	}

	dlog.Info(ctx, "Rebuilding node tree...")
	o := &Rebuilder{
		inner: btrfsutil.NewBrokenTrees(ctx, fs),

		orphans:      orphans,
		leaf2orphans: leaf2orphans,
		key2leaf:     *key2leaf,

		augments: make(map[btrfsprim.ObjID]containers.Set[btrfsvol.LogicalAddr]),
	}
	if err := o.rebuild(ctx); err != nil {
		return nil, err
	}

	return o.augments, nil
}

func (o *Rebuilder) rebuild(ctx context.Context) error {
	// TODO
	//btrfsutil.WalkAllTrees(ctx, o.inner)
	handleItem(o, ctx, 0, btrfstree.Item{})
	return nil
}

// err implements rebuildCallbacks.
func (o *Rebuilder) err(ctx context.Context, e error) {
	// TODO
}

// want implements rebuildCallbacks.
func (o *Rebuilder) want(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType) {
	// TODO
}

// wantOff implements rebuildCallbacks.
func (o *Rebuilder) wantOff(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, off uint64) {
	// TODO
}

// wantFunc implements rebuildCallbacks.
func (o *Rebuilder) wantFunc(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, fn func(btrfsitem.Item) bool) {
	// TODO
}

// func implements rebuildCallbacks.
//
// interval is [beg, end)
func (o *Rebuilder) wantCSum(ctx context.Context, beg, end btrfsvol.LogicalAddr) {
	// TODO
}

// wantFileExt implements rebuildCallbacks.
func (o *Rebuilder) wantFileExt(ctx context.Context, treeID btrfsprim.ObjID, ino btrfsprim.ObjID, size int64) {
	// TODO
}
