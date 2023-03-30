// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
)

type WalkAllTreesHandler struct {
	PreTree  func(name string, id btrfsprim.ObjID)
	BadTree  func(name string, id btrfsprim.ObjID, err error)
	Tree     btrfstree.TreeWalkHandler
	PostTree func(name string, id btrfsprim.ObjID)
}

// WalkAllTrees walks all trees in a btrfs.ReadableFS.  Rather than
// returning an error, it calls the appropriate "BadXXX" callback
// (BadTree, BadNode, BadItem) each time an error is encountered.
func WalkAllTrees(ctx context.Context, fs btrfs.ReadableFS, cbs WalkAllTreesHandler) {
	var treeName string

	trees := []struct {
		Name string
		ID   btrfsprim.ObjID
	}{
		{
			Name: "root tree",
			ID:   btrfsprim.ROOT_TREE_OBJECTID,
		},
		{
			Name: "chunk tree",
			ID:   btrfsprim.CHUNK_TREE_OBJECTID,
		},
		{
			Name: "log tree",
			ID:   btrfsprim.TREE_LOG_OBJECTID,
		},
		{
			Name: "block group tree",
			ID:   btrfsprim.BLOCK_GROUP_TREE_OBJECTID,
		},
	}
	origItem := cbs.Tree.Item
	cbs.Tree.Item = func(path btrfstree.Path, item btrfstree.Item) {
		if item.Key.ItemType == btrfsitem.ROOT_ITEM_KEY {
			trees = append(trees, struct {
				Name string
				ID   btrfsprim.ObjID
			}{
				Name: fmt.Sprintf("tree %v (via %v %v)",
					item.Key.ObjectID.Format(0), treeName, path),
				ID: item.Key.ObjectID,
			})
		}
		if origItem != nil {
			origItem(path, item)
		}
	}

	for i := 0; i < len(trees); i++ {
		treeInfo := trees[i]
		treeName = treeInfo.Name
		if cbs.PreTree != nil {
			cbs.PreTree(treeName, treeInfo.ID)
		}
		tree, err := fs.ForrestLookup(ctx, treeInfo.ID)
		switch {
		case err != nil:
			if cbs.BadTree != nil {
				cbs.BadTree(treeName, treeInfo.ID, err)
			}
		default:
			tree.TreeWalk(ctx, cbs.Tree)
		}
		if cbs.PostTree != nil {
			cbs.PostTree(treeName, treeInfo.ID)
		}
	}
}
