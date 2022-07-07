package btrfsmisc

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
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
	PreTree  func(name string, id btrfs.ObjID, laddr btrfsvol.LogicalAddr)
	PostTree func(name string, id btrfs.ObjID, laddr btrfsvol.LogicalAddr)
	// Callbacks for nodes or smaller
	UnsafeNodes bool
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
		ID   btrfs.ObjID
		Root btrfsvol.LogicalAddr
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
					ID   btrfs.ObjID
					Root btrfsvol.LogicalAddr
				}{
					Name: fmt.Sprintf("tree %v (via %v %v)",
						item.Head.Key.ObjectID.Format(0), treeName, path),
					ID:   item.Head.Key.ObjectID,
					Root: root.ByteNr,
				})
			}
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

	treeName = "superblock"
	superblock, err := fs.Superblock()
	if err != nil {
		handleErr(nil, err)
		return
	}

	treeName = "root tree"
	if cbs.PreTree != nil {
		cbs.PreTree(treeName, btrfs.ROOT_TREE_OBJECTID, superblock.Data.RootTree)
	}
	if err := fs.TreeWalk(superblock.Data.RootTree, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	if cbs.PostTree != nil {
		cbs.PostTree(treeName, btrfs.ROOT_TREE_OBJECTID, superblock.Data.RootTree)
	}

	treeName = "chunk tree"
	if cbs.PreTree != nil {
		cbs.PreTree(treeName, btrfs.CHUNK_TREE_OBJECTID, superblock.Data.ChunkTree)
	}
	if err := fs.TreeWalk(superblock.Data.ChunkTree, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	if cbs.PostTree != nil {
		cbs.PostTree(treeName, btrfs.CHUNK_TREE_OBJECTID, superblock.Data.ChunkTree)
	}

	treeName = "log tree"
	if cbs.PreTree != nil {
		cbs.PreTree(treeName, btrfs.TREE_LOG_OBJECTID, superblock.Data.LogTree)
	}
	if err := fs.TreeWalk(superblock.Data.LogTree, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	if cbs.PostTree != nil {
		cbs.PostTree(treeName, btrfs.TREE_LOG_OBJECTID, superblock.Data.LogTree)
	}

	treeName = "block group tree"
	if cbs.PreTree != nil {
		cbs.PreTree(treeName, btrfs.BLOCK_GROUP_TREE_OBJECTID, superblock.Data.BlockGroupRoot)
	}
	if err := fs.TreeWalk(superblock.Data.BlockGroupRoot, cbs.TreeWalkHandler); err != nil {
		handleErr(nil, err)
	}
	if cbs.PostTree != nil {
		cbs.PostTree(treeName, btrfs.BLOCK_GROUP_TREE_OBJECTID, superblock.Data.BlockGroupRoot)
	}

	for _, tree := range foundTrees {
		treeName = tree.Name
		if cbs.PreTree != nil {
			cbs.PreTree(treeName, tree.ID, tree.Root)
		}
		if err := fs.TreeWalk(tree.Root, cbs.TreeWalkHandler); err != nil {
			handleErr(nil, err)
		}
		if cbs.PostTree != nil {
			cbs.PostTree(treeName, tree.ID, tree.Root)
		}
	}
}
