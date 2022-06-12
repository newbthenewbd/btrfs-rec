package main

import (
	"fmt"
	iofs "io/fs"
	"sort"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func walkFS(fs *btrfs.FS, cbs btrfs.WalkTreeHandler, errCb func(error)) {
	var treeName string
	origErrCb := errCb
	errCb = func(err error) {
		origErrCb(fmt.Errorf("%v: %w", treeName, err))
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
				errCb(fmt.Errorf("%v: ROOT_ITEM_KEY is a %T, not a btrfsitem.Root", path, item.Body))
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
			errCb(fmt.Errorf("%v: %w", path, err))
		}
		if node != nil && origNode != nil {
			return origNode(path, node, nil)
		}
		return nil
	}

	treeName = "superblock"
	superblock, err := fs.Superblock()
	if err != nil {
		errCb(err)
		return
	}

	treeName = "root tree"
	if err := fs.WalkTree(superblock.Data.RootTree, cbs); err != nil {
		errCb(err)
	}

	treeName = "chunk tree"
	if err := fs.WalkTree(superblock.Data.ChunkTree, cbs); err != nil {
		errCb(err)
	}

	treeName = "log tree"
	if err := fs.WalkTree(superblock.Data.LogTree, cbs); err != nil {
		errCb(err)
	}

	treeName = "block group tree"
	if err := fs.WalkTree(superblock.Data.BlockGroupRoot, cbs); err != nil {
		errCb(err)
	}

	for _, tree := range foundTrees {
		treeName = tree.Name
		if err := fs.WalkTree(tree.Root, cbs); err != nil {
			errCb(err)
		}
	}
}

func pass2(fs *btrfs.FS, foundNodes map[btrfs.LogicalAddr]struct{}) {
	fmt.Printf("\nPass 2: orphaned nodes\n")

	visitedNodes := make(map[btrfs.LogicalAddr]struct{})
	walkFS(fs, btrfs.WalkTreeHandler{
		Node: func(path btrfs.WalkTreePath, node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
			visitedNodes[node.Addr] = struct{}{}
			return nil
		},
	}, func(err error) {
		fmt.Printf("Pass 2: walk FS: error: %v\n", err)
	})

	orphanedNodes := make(map[btrfs.LogicalAddr]int)
	for foundNode := range foundNodes {
		if _, visited := visitedNodes[foundNode]; !visited {
			orphanedNodes[foundNode] = 0
		}
	}

	for nodeAddr := range orphanedNodes {
		if err := fs.WalkTree(nodeAddr, btrfs.WalkTreeHandler{
			Node: func(path btrfs.WalkTreePath, node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
				nodeAddr := path[len(path)-1].NodeAddr
				orphanedNodes[nodeAddr] = orphanedNodes[nodeAddr] + 1
				visitCnt := orphanedNodes[nodeAddr]
				if visitCnt > 1 {
					return iofs.SkipDir
				}
				return nil
			},
		}); err != nil {
			fmt.Printf("Pass 2: walk orphans: error: %v\n", err)
		}
	}
	var orphanedRoots []btrfs.LogicalAddr
	for node, cnt := range orphanedNodes {
		switch cnt {
		case 0:
			panic("x")
		case 1:
			orphanedRoots = append(orphanedRoots, node)
		}
	}
	sort.Slice(orphanedRoots, func(i, j int) bool {
		return orphanedRoots[i] < orphanedRoots[j]
	})
	for _, node := range orphanedRoots {
		fmt.Printf("Pass 2: orphaned root: %v\n", node)
	}
}
