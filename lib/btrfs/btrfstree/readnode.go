// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
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

// NodeExpectations returns the address to read and the expectations
// to have when reading the node pointed to by this Path.
//
// `ok` is false if the path is empty or if this Path points to an
// item rather than a node.
func (path Path) NodeExpectations(fs NodeFile) (_ btrfsvol.LogicalAddr, _ NodeExpectations, ok bool) {
	if path.Node(-1).ToNodeAddr == 0 && path.Node(-1).ToNodeGeneration == 0 && path.Node(-1).ToNodeLevel == 0 {
		return 0, NodeExpectations{}, false
	}

	checkOwner := func(owner btrfsprim.ObjID, gen btrfsprim.Generation) error {
		var treeParents []btrfsprim.ObjID

		tree := path.Node(-1).FromTree
		for {
			if owner == tree {
				// OK!
				return nil
			}

			treeParents = append(treeParents, tree)
			parent, parentGen, parentOK := fs.ParentTree(tree)
			if !parentOK {
				// Failed look up parent info; fail open.
				return nil
			}

			if parent == 0 {
				// End of the line.
				return fmt.Errorf("expected owner in %v but claims to have owner=%v",
					treeParents, owner)
			}
			if gen > parentGen {
				return fmt.Errorf("claimed owner=%v might be acceptable in this tree (if generation<=%v) but not with claimed generation=%v",
					owner, parentGen, gen)
			}
			tree = parent
		}
	}

	return path.Node(-1).ToNodeAddr, NodeExpectations{
		LAddr:      containers.OptionalValue(path.Node(-1).ToNodeAddr),
		Level:      containers.OptionalValue(path.Node(-1).ToNodeLevel),
		Generation: containers.OptionalValue(path.Node(-1).ToNodeGeneration),
		Owner:      checkOwner,
		MinItem:    containers.OptionalValue(path.Node(-1).ToKey),
		MaxItem:    containers.OptionalValue(path.Node(-1).ToMaxKey),
	}, true
}
