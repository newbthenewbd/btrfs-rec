// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type RebuiltTrees struct {
	inner   *btrfs.FS
	uuidMap uuidMap
	nodes   map[btrfsvol.LogicalAddr]*RebuiltNode
}

type _FS interface {
	diskio.File[btrfsvol.LogicalAddr]
	btrfstree.NodeFile
	btrfstree.NodeSource
	btrfstree.TreeOperator
}

// diskio.File

func (fs *RebuiltTrees) Name() string               { return fs.inner.Name() }
func (fs *RebuiltTrees) Size() btrfsvol.LogicalAddr { return fs.inner.Size() }
func (fs *RebuiltTrees) Close() error               { return fs.inner.Close() }
func (fs *RebuiltTrees) ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return 0, err
	}
	if rebuilt, ok := fs.nodes[off]; ok && len(p) == int(sb.NodeSize) {
		rebuilt.Node.Head.Checksum, err = rebuilt.Node.CalculateChecksum()
		if err != nil {
			panic(fmt.Errorf("should not happen: %w", err))
		}
		bs, err := rebuilt.Node.MarshalBinary()
		if err != nil {
			panic(fmt.Errorf("should not happen: %w", err))
		}
		if len(p) != len(bs) {
			panic(fmt.Errorf("should not happen: sb.NodeSize=%v but node marshaled to %v", sb.NodeSize, len(bs)))
		}
		return copy(p, bs), nil
	}
	return fs.inner.ReadAt(p, off)
}
func (fs *RebuiltTrees) WriteAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return fs.inner.WriteAt(p, off)
}

// btrfstree.NodeFile

func (fs *RebuiltTrees) ParentTree(tree btrfsprim.ObjID) (btrfsprim.ObjID, bool) {
	if tree < btrfsprim.FIRST_FREE_OBJECTID || tree > btrfsprim.LAST_FREE_OBJECTID {
		// no parent
		return 0, true
	}
	parentUUID, ok := fs.uuidMap.TreeParent[tree]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	if parentUUID == (btrfsprim.UUID{}) {
		// no parent
		return 0, true
	}
	parentObjID, ok := fs.uuidMap.UUID2ObjID[parentUUID]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	return parentObjID, true
}

// btrfstree.NodeSource

func (fs *RebuiltTrees) Superblock() (*btrfstree.Superblock, error) { return fs.inner.Superblock() }
func (fs *RebuiltTrees) ReadNode(path btrfstree.TreePath) (*diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], error) {
	return btrfstree.FSReadNode(fs, path)
}

// btrfstree.TreeOperator

func (fs *RebuiltTrees) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*btrfstree.TreeError), cbs btrfstree.TreeWalkHandler) {
	btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeWalk(ctx, treeID, errHandle, cbs)
}
func (fs *RebuiltTrees) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeLookup(treeID, key)
}
func (fs *RebuiltTrees) TreeSearch(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) (btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeSearch(treeID, fn)
}
func (fs *RebuiltTrees) TreeSearchAll(treeID btrfsprim.ObjID, fn func(key btrfsprim.Key, size uint32) int) ([]btrfstree.Item, error) {
	return btrfstree.TreeOperatorImpl{NodeSource: fs}.TreeSearchAll(treeID, fn)
}
