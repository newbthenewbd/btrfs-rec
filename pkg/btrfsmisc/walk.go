package btrfsmisc

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
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

type WalkFSHandler struct {
	Err func(error)
	// Callbacks for entire trees
	PreTree  func(name string, laddr btrfs.LogicalAddr)
	PostTree func(name string, laddr btrfs.LogicalAddr)
	// Callbacks for nodes or smaller
	btrfs.TreeWalkHandler
}

// WalkFS walks all trees in a *btrfs.FS.  Rather than returning an
// error, it calls errCb each time an error is encountered.  The error
// will always be of type WalkErr.
func WalkFS(fs *btrfs.FS, cbs WalkFSHandler) {
	var treeName string
	handleErr := func(path btrfs.TreePath, err error) {
		cbs.Err(WalkErr{
			TreeName: treeName,
			Path:     path,
			Err:      err,
		})
	}

	var foundTrees []struct {
		Name string
		Root btrfs.LogicalAddr
	}
	origItem := cbs.Item
	cbs.Item = func(path btrfs.TreePath, item btrfs.Item) error {
		if item.Head.Key.ItemType == btrfsitem.ROOT_ITEM_KEY {
			root, ok := item.Body.(btrfsitem.Root)
			if !ok {
				handleErr(path, fmt.Errorf("ROOT_ITEM_KEY is a %T, not a btrfsitem.Root", item.Body))
			} else {
				foundTrees = append(foundTrees, struct {
					Name string
					Root btrfs.LogicalAddr
				}{
					Name: fmt.Sprintf("tree %v (via %v %v)",
						item.Head.Key.ObjectID.Format(0), treeName, path),
					Root: root.ByteNr,
				})
			}
		}
		if origItem != nil {
			return origItem(path, item)
		}
		return nil
	}

	origNode := cbs.Node
	cbs.Node = func(path btrfs.TreePath, node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
		if err != nil {
			handleErr(path, err)
		}
		if node != nil && origNode != nil {
			return origNode(path, node, nil)
		}
		return nil
	}

	treeName = "superblock"
	superblock, err := fs.Superblock()
	if err != nil {
		handleErr(nil, err)
		return
	}

	treeName = "root tree"
	cbs.PreTree(treeName, superblock.Data.RootTree)
	if err := fs.TreeWalk(superblock.Data.RootTree, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	cbs.PostTree(treeName, superblock.Data.RootTree)

	treeName = "chunk tree"
	cbs.PreTree(treeName, superblock.Data.ChunkTree)
	if err := fs.TreeWalk(superblock.Data.ChunkTree, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	cbs.PostTree(treeName, superblock.Data.ChunkTree)

	treeName = "log tree"
	cbs.PreTree(treeName, superblock.Data.LogTree)
	if err := fs.TreeWalk(superblock.Data.LogTree, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	cbs.PostTree(treeName, superblock.Data.LogTree)

	treeName = "block group tree"
	cbs.PreTree(treeName, superblock.Data.BlockGroupRoot)
	if err := fs.TreeWalk(superblock.Data.BlockGroupRoot, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	cbs.PostTree(treeName, superblock.Data.BlockGroupRoot)

	for _, tree := range foundTrees {
		treeName = tree.Name
		cbs.PreTree(treeName, tree.Root)
		if err := fs.TreeWalk(tree.Root, cbs.TreeWalkHandler); err != nil {
			handleErr(nil, err)
		}
		cbs.PostTree(treeName, tree.Root)
	}
}
