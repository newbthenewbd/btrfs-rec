package main

import (
	"fmt"
	iofs "io/fs"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func pass2(fs *btrfs.FS, foundNodes map[btrfs.LogicalAddr]struct{}) {
	fmt.Printf("\nPass 2: orphaned nodes\n")

	visitedNodes := make(map[btrfs.LogicalAddr]struct{})
	btrfsmisc.WalkFS(fs, btrfsmisc.WalkFSHandler{
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
				visitedNodes[node.Addr] = struct{}{}
				return nil
			},
		},
		Err: func(err error) {
			fmt.Printf("Pass 2: walk FS: error: %v\n", err)
		},
	})

	orphanedNodes := make(map[btrfs.LogicalAddr]int)
	for foundNode := range foundNodes {
		if _, visited := visitedNodes[foundNode]; !visited {
			orphanedNodes[foundNode] = 0
		}
	}

	orphanedRoots := make(map[btrfs.LogicalAddr]struct{}, len(orphanedNodes))
	for node := range orphanedNodes {
		orphanedRoots[node] = struct{}{}
	}
	for potentialRoot := range orphanedRoots {
		if err := fs.TreeWalk(potentialRoot, btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, _ *util.Ref[btrfs.LogicalAddr, btrfs.Node], _ error) error {
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

	for _, node := range util.SortedMapKeys(orphanedRoots) {
		fmt.Printf("Pass 2: orphaned root: %v\n", node)
	}
}
