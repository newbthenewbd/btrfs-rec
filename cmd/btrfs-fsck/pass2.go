package main

import (
	"fmt"
	iofs "io/fs"
	"sort"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func pass2(fs *btrfs.FS, foundNodes map[btrfs.LogicalAddr]struct{}) {
	if true {
		return
	}
	fmt.Printf("\nPass 2: orphaned nodes\n")

	visitedNodes := make(map[btrfs.LogicalAddr]struct{})
	btrfsmisc.WalkFS(fs, btrfs.WalkTreeHandler{
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
