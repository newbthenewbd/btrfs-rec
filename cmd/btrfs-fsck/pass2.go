package main

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func walkFS(fs *btrfs.FS, cbs btrfs.WalkTreeHandler, errCb func(error)) {
	origItem := cbs.Item
	cbs.Item = func(key btrfs.Key, body btrfsitem.Item) error {
		if key.ItemType == btrfsitem.ROOT_ITEM_KEY {
			root, ok := body.(btrfsitem.Root)
			if !ok {
				errCb(fmt.Errorf("ROOT_ITEM_KEY is a %T, not a btrfsitem.Root", body))
			} else if err := fs.WalkTree(root.ByteNr, cbs); err != nil {
				errCb(err)
			}
		}
		if origItem != nil {
			return origItem(key, body)
		}
		return nil
	}

	superblock, err := fs.Superblock()
	if err != nil {
		errCb(err)
		return
	}

	if err := fs.WalkTree(superblock.Data.RootTree, cbs); err != nil {
		errCb(err)
	}
	if err := fs.WalkTree(superblock.Data.ChunkTree, cbs); err != nil {
		errCb(err)
	}
	if err := fs.WalkTree(superblock.Data.LogTree, cbs); err != nil {
		errCb(err)
	}
	if err := fs.WalkTree(superblock.Data.BlockGroupRoot, cbs); err != nil {
		errCb(err)
	}
}

func pass2(fs *btrfs.FS, foundNodes map[btrfs.LogicalAddr]struct{}) {
	fmt.Printf("\nPass 2: orphaned nodes\n")

	visitedNodes := make(map[btrfs.LogicalAddr]struct{})
	walkFS(fs, btrfs.WalkTreeHandler{
		Node: func(node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
			if err != nil {
				fmt.Printf("Pass 2: error: %v\n", err)
			}
			if node != nil {
				visitedNodes[node.Addr] = struct{}{}
			}
			return nil
		},
	}, func(err error) {
		fmt.Printf("Pass 2: error: %v\n", err)
	})

	orphanedNodes := make(map[btrfs.LogicalAddr]struct{})
	for foundNode := range foundNodes {
		if _, visited := visitedNodes[foundNode]; !visited {
			orphanedNodes[foundNode] = struct{}{}
		}
	}

	//fmt.Printf("Pass 2: orphanedNodes=%#v\n", orphanedNodes)
}
