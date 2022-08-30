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

func lostAndFoundNodes(ctx context.Context, fs _FS, nodeScanResults btrfsinspect.ScanDevicesResult) (map[btrfsvol.LogicalAddr]struct{}, error) {
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

	attachedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: btrfstree.TreeWalkHandler{
			Node: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
				addr := path.Node(-1).ToNodeAddr
				if _, alreadyVisited := attachedNodes[addr]; alreadyVisited {
					// Can happen because of COW subvolumes;
					// this is really a DAG not a tree.
					return iofs.SkipDir
				}
				attachedNodes[addr] = struct{}{}
				progress(len(attachedNodes))
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
			if _, attached := attachedNodes[laddr]; !attached {
				orphanedNodes[laddr] = 0
			}
		}
	}
	dlog.Infof(ctx,
		"... (finished processing %v attached nodes, proceeding to process %v lost nodes, for a total of %v)",
		len(attachedNodes), len(orphanedNodes), len(attachedNodes)+len(orphanedNodes))

	// 'orphanedRoots' is a subset of 'orphanedNodes'; start with
	// it as the complete orphanedNodes, and then remove entries.
	orphanedRoots := make(map[btrfsvol.LogicalAddr]struct{}, len(orphanedNodes))
	for node := range orphanedNodes {
		orphanedRoots[node] = struct{}{}
	}
	done := len(attachedNodes)
	for potentialRoot := range orphanedRoots {
		done++
		progress(done)
		if orphanedNodes[potentialRoot] > 1 {
			continue
		}
		walkCtx, cancel := context.WithCancel(ctx)
		walkFromNode(walkCtx, fs, potentialRoot,
			func(err *btrfstree.TreeError) {
				// do nothing
			},
			btrfstree.TreeWalkHandler{
				PreNode: func(path btrfstree.TreePath) error {
					nodeAddr := path.Node(-1).ToNodeAddr
					if nodeAddr != potentialRoot {
						delete(orphanedRoots, nodeAddr)
					}
					visitCnt := orphanedNodes[nodeAddr] + 1
					orphanedNodes[nodeAddr] = visitCnt
					if visitCnt > 1 {
						cancel()
					}
					return nil
				},
			},
		)
	}

	return orphanedRoots, nil
}
