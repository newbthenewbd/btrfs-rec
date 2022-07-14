// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func pass2(ctx context.Context, fs *btrfs.FS, foundNodes map[btrfsvol.LogicalAddr]struct{}) {
	fmt.Printf("\nPass 2: orphaned nodes\n")

	visitedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]) error {
				visitedNodes[node.Addr] = struct{}{}
				return nil
			},
		},
		Err: func(err *btrfsutil.WalkError) {
			fmt.Printf("Pass 2: walk FS: error: %v\n", err)
		},
	})

	orphanedNodes := make(map[btrfsvol.LogicalAddr]int)
	for foundNode := range foundNodes {
		if _, visited := visitedNodes[foundNode]; !visited {
			orphanedNodes[foundNode] = 0
		}
	}

	orphanedRoots := make(map[btrfsvol.LogicalAddr]struct{}, len(orphanedNodes))
	for node := range orphanedNodes {
		orphanedRoots[node] = struct{}{}
	}
	/*
		for potentialRoot := range orphanedRoots {
			if err := fs.TreeWalk(potentialRoot, btrfs.TreeWalkHandler{
				Node: func(path btrfs.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node], _ error) error {
					nodeAddr := path[len(path)-1].NodeAddr
					if nodeAddr != potentialRoot {
						delete(orphanedRoots, nodeAddr)
					}
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

		for _, node := range maps.SortedKeys(orphanedRoots) {
			fmt.Printf("Pass 2: orphaned root: %v\n", node)
		}
	*/
}
