// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

// This file is ordered from low-level to high-level.

// btrfstree.NodeSource ////////////////////////////////////////////////////////

type nodeCacheEntry struct {
	node *btrfstree.Node
	err  error
}

// AcquireNode implements btrfstree.NodeSource.
func (fs *FS) AcquireNode(ctx context.Context, addr btrfsvol.LogicalAddr, exp btrfstree.NodeExpectations) (*btrfstree.Node, error) {
	if fs.cacheNodes == nil {
		fs.cacheNodes = containers.NewARCache[btrfsvol.LogicalAddr, nodeCacheEntry](
			textui.Tunable(4*(btrfstree.MaxLevel+1)),
			containers.SourceFunc[btrfsvol.LogicalAddr, nodeCacheEntry](fs.readNode),
		)
	}

	nodeEntry := fs.cacheNodes.Acquire(ctx, addr)
	if nodeEntry.err != nil {
		err := nodeEntry.err
		fs.cacheNodes.Release(addr)
		return nil, err
	}

	if nodeEntry.node != nil {
		if err := exp.Check(nodeEntry.node); err != nil {
			fs.cacheNodes.Release(addr)
			return nil, fmt.Errorf("btrfstree.ReadNode: node@%v: %w", addr, err) // fmt.Errorf("btrfs.FS.AcquireNode: node@%v: %w", addr, err)
		}
	}

	return nodeEntry.node, nil
}

// ReleaseNode implements btrfstree.NodeSource.
func (fs *FS) ReleaseNode(node *btrfstree.Node) {
	if node == nil {
		return
	}
	fs.cacheNodes.Release(node.Head.Addr)
}

func (fs *FS) readNode(_ context.Context, addr btrfsvol.LogicalAddr, nodeEntry *nodeCacheEntry) {
	nodeEntry.node.RawFree()
	nodeEntry.node = nil

	sb, err := fs.Superblock()
	if err != nil {
		nodeEntry.err = err
		return
	}

	nodeEntry.node, nodeEntry.err = btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, addr)
}

var _ btrfstree.NodeSource = (*FS)(nil)

// btrfstree.Forrest ///////////////////////////////////////////////////////////

// RawTree is a variant of ForrestLookup that returns a concrete type
// instead of an interface.
func (fs *FS) RawTree(ctx context.Context, treeID btrfsprim.ObjID) (*btrfstree.RawTree, error) {
	return btrfstree.RawForrest{NodeSource: fs}.RawTree(ctx, treeID)
}

// ForrestLookup implements btree.Forrest.
func (fs *FS) ForrestLookup(ctx context.Context, treeID btrfsprim.ObjID) (btrfstree.Tree, error) {
	return btrfstree.RawForrest{NodeSource: fs}.ForrestLookup(ctx, treeID)
}

var _ btrfstree.Forrest = (*FS)(nil)

// ReadableFS //////////////////////////////////////////////////////////////////

type ReadableFS interface {
	Name() string

	// For reading btrees.
	btrfstree.Forrest

	// For reading the superblock and raw nodes.
	btrfstree.NodeSource

	// For reading file contents.
	diskio.ReaderAt[btrfsvol.LogicalAddr]
}

var _ ReadableFS = (*FS)(nil)
