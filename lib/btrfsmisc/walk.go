package btrfsmisc

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

type WalkErr struct {
	TreeName string
	Path     btrfs.TreePath
	Err      error
}

func (e WalkErr) Unwrap() error { return e.Err }

func (e WalkErr) Error() string {
	if len(e.Path) == 0 {
		return fmt.Sprintf("%v: %v", e.TreeName, e.Err)
	}
	return fmt.Sprintf("%v: %v: %v", e.TreeName, e.Path, e.Err)
}

type WalkAllTreesHandler struct {
	Err func(error)
	// Callbacks for entire trees
	PreTree  func(name string, id btrfs.ObjID)
	PostTree func(name string, id btrfs.ObjID)
	// Callbacks for nodes or smaller
	UnsafeNodes bool
	btrfs.TreeWalkHandler
}

// WalkAllTrees walks all trees in a *btrfs.FS.  Rather than returning
// an error, it calls errCb each time an error is encountered.  The
// error will always be of type WalkErr.
func WalkAllTrees(fs *btrfs.FS, cbs WalkAllTreesHandler) {
	var treeName string
	handleErr := func(path btrfs.TreePath, err error) {
		cbs.Err(WalkErr{
			TreeName: treeName,
			Path:     path,
			Err:      err,
		})
	}

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
		if item.Head.Key.ItemType == btrfsitem.ROOT_ITEM_KEY {
			trees = append(trees, struct {
				Name string
				ID   btrfs.ObjID
			}{
				Name: fmt.Sprintf("tree %v (via %v %v)",
					item.Head.Key.ObjectID.Format(0), treeName, path),
				ID: item.Head.Key.ObjectID,
			})
		}
		if origItem != nil {
			return origItem(path, item)
		}
		return nil
	}

	if !cbs.UnsafeNodes {
		origNode := cbs.Node
		cbs.Node = func(path btrfs.TreePath, node *util.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
			if err != nil {
				handleErr(path, err)
			}
			if node != nil && origNode != nil {
				return origNode(path, node, nil)
			}
			return nil
		}
	}

	for i := 0; i < len(trees); i++ {
		tree := trees[i]
		treeName = tree.Name
		if cbs.PreTree != nil {
			cbs.PreTree(treeName, tree.ID)
		}
		if err := fs.TreeWalk(tree.ID, cbs.TreeWalkHandler); err != nil {
			handleErr(nil, err)
		}
		if cbs.PostTree != nil {
			cbs.PostTree(treeName, tree.ID)
		}
	}
}
