package main

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func walkFS(fs *btrfs.FS, cbs btrfs.WalkTreeHandler, errCb func(error)) {
	origItem := cbs.Item
	cbs.Item = func(path btrfs.WalkTreePath, item btrfs.Item) error {
		if item.Head.Key.ItemType == btrfsitem.ROOT_ITEM_KEY {
			root, ok := item.Body.(btrfsitem.Root)
			if !ok {
				errCb(fmt.Errorf("%v: ROOT_ITEM_KEY is a %T, not a btrfsitem.Root", path, item.Body))
			} else if err := fs.WalkTree(root.ByteNr, cbs); err != nil {
				errCb(fmt.Errorf("%v: tree %v: %w", path, item.Head.Key.ObjectID.Format(0), err))
			}
		}
		if origItem != nil {
			return origItem(path, item)
		}
		return nil
	}

	superblock, err := fs.Superblock()
	if err != nil {
		errCb(fmt.Errorf("superblock: %w", err))
		return
	}

	if err := fs.WalkTree(superblock.Data.RootTree, cbs); err != nil {
		errCb(fmt.Errorf("root tree: %w", err))
	}
	if err := fs.WalkTree(superblock.Data.ChunkTree, cbs); err != nil {
		errCb(fmt.Errorf("chunk tree: %w", err))
	}
	if err := fs.WalkTree(superblock.Data.LogTree, cbs); err != nil {
		errCb(fmt.Errorf("log tree: %w", err))
	}
	if err := fs.WalkTree(superblock.Data.BlockGroupRoot, cbs); err != nil {
		errCb(fmt.Errorf("block group tree: %w", err))
	}
}

func pass2(fs *btrfs.FS, foundNodes map[btrfs.LogicalAddr]struct{}) {
	fmt.Printf("\nPass 2: orphaned nodes\n")

	visitedNodes := make(map[btrfs.LogicalAddr]struct{})
	walkFS(fs, btrfs.WalkTreeHandler{
		Node: func(path btrfs.WalkTreePath, node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
			if err != nil {
				fmt.Printf("Pass 2: node error: %v: %v\n", path, err)
			}
			if node != nil {
				visitedNodes[node.Addr] = struct{}{}
			}
			return nil
		},
	}, func(err error) {
		fmt.Printf("Pass 2: walk error: %v\n", err)
	})

	orphanedNodes := make(map[btrfs.LogicalAddr]struct{})
	for foundNode := range foundNodes {
		if _, visited := visitedNodes[foundNode]; !visited {
			orphanedNodes[foundNode] = struct{}{}
		}
	}

	//fmt.Printf("Pass 2: orphanedNodes=%#v\n", orphanedNodes)
}
