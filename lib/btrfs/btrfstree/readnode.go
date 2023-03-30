// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type NodeFile interface {
	diskio.ReaderAt[btrfsvol.LogicalAddr]

	// ParentTree, given a tree ID, returns that tree's parent
	// tree, if it has one.
	//
	//  - non-zero, ?, true : the parent tree ID
	//
	//  - 0, 0, true : the tree does not have a parent
	//
	//  - ?, ?, false : the tree's parent information could not be
	//    looked up
	ParentTree(btrfsprim.ObjID) (btrfsprim.ObjID, btrfsprim.Generation, bool)
}
