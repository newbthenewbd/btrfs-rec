// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"errors"
	iofs "io/fs"
	"strings"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

// lostAndFoundNodes returns the set of nodes don't have another node
// claiming it as a child.
func lostAndFoundNodes(ctx context.Context, fs _FS, nodeScanResults btrfsinspect.ScanDevicesResult) (map[btrfsvol.LogicalAddr]struct{}, error) {
	dlog.Info(ctx, "Identifying lost+found nodes...")

	lastPct := -1
	total := countNodes(nodeScanResults)
	progress := func(done int) {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}

	visitedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: btrfstree.TreeWalkHandler{
			// Don't use `PreNode` because we don't want to run this on bad nodes.
			Node: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
				addr := path.Node(-1).ToNodeAddr
				if _, alreadyVisited := visitedNodes[addr]; alreadyVisited {
					// Can happen because of COW subvolumes;
					// this is really a DAG not a tree.
					return iofs.SkipDir
				}
				visitedNodes[addr] = struct{}{}
				progress(len(visitedNodes))
				return nil
			},
		},
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
			if !errors.Is(err, btrfstree.ErrNotANode) && !strings.Contains(err.Error(), "read: could not map logical address") {
				dlog.Errorf(ctx, "dbg walk err: %v", err)
			}
		},
	})

	orphanedNodes := make(map[btrfsvol.LogicalAddr]int)
	for _, devResults := range nodeScanResults {
		for laddr := range devResults.FoundNodes {
			if _, attached := visitedNodes[laddr]; !attached {
				orphanedNodes[laddr] = 0
			}
		}
	}
	if len(visitedNodes)+len(orphanedNodes) != total {
		panic("should not happen")
	}
	dlog.Infof(ctx,
		"... (finished processing %v attached nodes, proceeding to process %v lost nodes, for a total of %v)",
		len(visitedNodes), len(orphanedNodes), len(visitedNodes)+len(orphanedNodes))

	// 'orphanedRoots' is a subset of 'orphanedNodes'; start with
	// it as the complete orphanedNodes, and then remove entries.
	orphanedRoots := make(map[btrfsvol.LogicalAddr]struct{}, len(orphanedNodes))
	for node := range orphanedNodes {
		orphanedRoots[node] = struct{}{}
	}
	for potentialRoot := range orphanedNodes {
		if orphanedNodes[potentialRoot] > 1 {
			continue
		}
		walkFromNode(ctx, fs, potentialRoot,
			func(err *btrfstree.TreeError) {
				// do nothing
			},
			btrfstree.TreeWalkHandler{
				// Don't use `PreNode` because we don't want to run this on bad
				// nodes (it'd screw up `len(visitedNodes)`).
				Node: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
					addr := path.Node(-1).ToNodeAddr
					if addr != potentialRoot {
						delete(orphanedRoots, addr)
					}
					if _, alreadyVisited := visitedNodes[addr]; alreadyVisited {
						return iofs.SkipDir
					}
					visitedNodes[addr] = struct{}{}
					progress(len(visitedNodes))
					return nil
				},
			},
		)
	}

	if len(visitedNodes) != total {
		panic("should not happen")
	}

	dlog.Infof(ctx, "... identified %d lost+found nodes", len(orphanedRoots))
	return orphanedRoots, nil
}
