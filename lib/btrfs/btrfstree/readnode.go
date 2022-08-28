// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
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

// FSReadNode is a utility function to help with implementing the
// 'NodeSource' interface.
func FSReadNode(
	fs interface {
		diskio.File[btrfsvol.LogicalAddr]
		Superblock() (*Superblock, error)
		ParentTree(btrfsprim.ObjID) (btrfsprim.ObjID, bool)
	},
	path TreePath,
) (*diskio.Ref[btrfsvol.LogicalAddr, Node], error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, fmt.Errorf("btrfs.FS.ReadNode: %w", err)
	}

	var treeParents []btrfsprim.ObjID
	checkOwner := func(owner btrfsprim.ObjID) error {
		exp := path.Node(-1).FromTree
		for {
			if owner == exp {
				return nil
			}
			treeParents = append(treeParents, exp)
			var ok bool
			exp, ok = fs.ParentTree(exp)
			if !ok {
				return fmt.Errorf("expected owner in %v but claims to have owner=%v",
					treeParents, owner)
			}
		}
	}

	return ReadNode[btrfsvol.LogicalAddr](fs, *sb, path.Node(-1).ToNodeAddr, NodeExpectations{
		LAddr:         containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: path.Node(-1).ToNodeAddr},
		Level:         containers.Optional[uint8]{OK: true, Val: path.Node(-1).ToNodeLevel},
		MaxGeneration: containers.Optional[btrfsprim.Generation]{OK: true, Val: path.Node(-1).FromGeneration},
		Owner:         checkOwner,
	})
}
