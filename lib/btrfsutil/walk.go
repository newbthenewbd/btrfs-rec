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
)

type WalkError struct {
	TreeName string
	Err      *btrfstree.TreeError
}

func (e *WalkError) Unwrap() error { return e.Err }

func (e *WalkError) Error() string {
	return fmt.Sprintf("%v: %v", e.TreeName, e.Err)
}

type WalkAllTreesHandler struct {
	Err func(*WalkError)
	// Callbacks for entire trees
	PreTree  func(name string, id btrfsprim.ObjID)
	PostTree func(name string, id btrfsprim.ObjID)
	// Callbacks for nodes or smaller
	btrfstree.TreeWalkHandler
}

// WalkAllTrees walks all trees in a *btrfs.FS.  Rather than returning
// an error, it calls errCb each time an error is encountered.  The
// error will always be of type WalkError.
func WalkAllTrees(ctx context.Context, fs btrfstree.TreeOperator, cbs WalkAllTreesHandler) {
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
	origItem := cbs.Item
	cbs.Item = func(path btrfstree.Path, item btrfstree.Item) error {
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
			return origItem(path, item)
		}
		return nil
	}

	for i := 0; i < len(trees); i++ {
		tree := trees[i]
		treeName = tree.Name
		if cbs.PreTree != nil {
			cbs.PreTree(treeName, tree.ID)
		}
		fs.TreeWalk(
			ctx,
			tree.ID,
			func(err *btrfstree.TreeError) { cbs.Err(&WalkError{TreeName: treeName, Err: err}) },
			cbs.TreeWalkHandler,
		)
		if cbs.PostTree != nil {
			cbs.PostTree(treeName, tree.ID)
		}
	}
}
