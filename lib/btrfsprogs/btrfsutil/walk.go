// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
)

type WalkError struct {
	TreeName string
	Err      *btrfs.TreeError
}

func (e *WalkError) Unwrap() error { return e.Err }

func (e *WalkError) Error() string {
	return fmt.Sprintf("%v: %v", e.TreeName, e.Err)
}

type WalkAllTreesHandler struct {
	Err func(*WalkError)
	// Callbacks for entire trees
	PreTree  func(name string, id btrfs.ObjID)
	PostTree func(name string, id btrfs.ObjID)
	// Callbacks for nodes or smaller
	btrfs.TreeWalkHandler
}

// WalkAllTrees walks all trees in a *btrfs.FS.  Rather than returning
// an error, it calls errCb each time an error is encountered.  The
// error will always be of type WalkError.
func WalkAllTrees(fs *btrfs.FS, cbs WalkAllTreesHandler) {
	var treeName string

	trees := []struct {
		Name string
		ID   btrfs.ObjID
	}{
		{
			Name: "root tree",
			ID:   btrfs.ROOT_TREE_OBJECTID,
		},
		{
			Name: "chunk tree",
			ID:   btrfs.CHUNK_TREE_OBJECTID,
		},
		{
			Name: "log tree",
			ID:   btrfs.TREE_LOG_OBJECTID,
		},
		{
			Name: "block group tree",
			ID:   btrfs.BLOCK_GROUP_TREE_OBJECTID,
		},
	}
	origItem := cbs.Item
	cbs.Item = func(path btrfs.TreePath, item btrfs.Item) error {
		if item.Key.ItemType == btrfsitem.ROOT_ITEM_KEY {
			trees = append(trees, struct {
				Name string
				ID   btrfs.ObjID
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
			tree.ID,
			func(err *btrfs.TreeError) { cbs.Err(&WalkError{TreeName: treeName, Err: err}) },
			cbs.TreeWalkHandler,
		)
		if cbs.PostTree != nil {
			cbs.PostTree(treeName, tree.ID)
		}
	}
}
