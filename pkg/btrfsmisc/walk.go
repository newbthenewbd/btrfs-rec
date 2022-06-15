package btrfsmisc

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type WalkErr struct {
	TreeName string
	Path     btrfs.WalkTreePath
	Err      error
}

func (e WalkErr) Unwrap() error { return e.Err }

func (e WalkErr) Error() string {
	if len(e.Path) == 0 {
		return fmt.Sprintf("%v: %v", e.TreeName, e.Err)
	}
	return fmt.Sprintf("%v: %v: %v", e.TreeName, e.Path, e.Err)
}

// WalkFS walks all trees in a *btrfs.FS.  Rather than returning an
// error, it calls errCb each time an error is encountered.  The error
// will always be of type WalkErr.
func WalkFS(fs *btrfs.FS, cbs btrfs.WalkTreeHandler, errCb func(error)) {
	var treeName string
	handleErr := func(path btrfs.WalkTreePath, err error) {
		errCb(WalkErr{
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
	cbs.Item = func(path btrfs.WalkTreePath, item btrfs.Item) error {
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
	cbs.Node = func(path btrfs.WalkTreePath, node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
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
	if err := fs.WalkTree(superblock.Data.RootTree, cbs); err != nil {
		handleErr(nil, err)
	}

	treeName = "chunk tree"
	if err := fs.WalkTree(superblock.Data.ChunkTree, cbs); err != nil {
		handleErr(nil, err)
	}

	treeName = "log tree"
	if err := fs.WalkTree(superblock.Data.LogTree, cbs); err != nil {
		handleErr(nil, err)
	}

	treeName = "block group tree"
	if err := fs.WalkTree(superblock.Data.BlockGroupRoot, cbs); err != nil {
		handleErr(nil, err)
	}

	for _, tree := range foundTrees {
		treeName = tree.Name
		if err := fs.WalkTree(tree.Root, cbs); err != nil {
			handleErr(nil, err)
		}
	}
}
