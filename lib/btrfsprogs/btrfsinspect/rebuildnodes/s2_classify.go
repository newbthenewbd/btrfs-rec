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
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type badNode struct {
	Err  string
	Path btrfstree.TreePath
}

// classifyNodes returns
//
//  1. the set of nodes don't have another node claiming it as a child, and
//  2. the list of bad nodes (in no particular order)
func classifyNodes(ctx context.Context, fs _FS, scanResults btrfsinspect.ScanDevicesResult) (
	orphanedNodes map[btrfsvol.LogicalAddr]struct{},
	badNodes []badNode,
	err error,
) {
	dlog.Info(ctx, "Walking trees to identify orphan and broken nodes...")

	lastPct := -1
	total := countNodes(scanResults)
	visitedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	progress := func() {
		done := len(visitedNodes)
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}

	walkHandler := btrfstree.TreeWalkHandler{
		PreNode: func(path btrfstree.TreePath) error {
			addr := path.Node(-1).ToNodeAddr
			if _, alreadyVisited := visitedNodes[addr]; alreadyVisited {
				// Can happen because of COW subvolumes;
				// this is really a DAG not a tree.
				return iofs.SkipDir
			}
			return nil
		},
		Node: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
			addr := path.Node(-1).ToNodeAddr
			visitedNodes[addr] = struct{}{}
			progress()
			return nil
		},
		BadNode: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
			badNodes = append(badNodes, badNode{
				Err:  err.Error(),
				Path: path.DeepCopy(),
			})
			return err
		},
	}

	progress()
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: walkHandler,
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
			if !errors.Is(err, btrfstree.ErrNotANode) && !strings.Contains(err.Error(), "read: could not map logical address") {
				dlog.Errorf(ctx, "dbg walk err: %v", err)
			}
		},
	})

	// Start with 'orphanedRoots' as a complete set of all orphaned nodes, and then delete
	// non-root entries from it.
	orphanedNodes = make(map[btrfsvol.LogicalAddr]struct{})
	for _, devResults := range scanResults {
		for laddr := range devResults.FoundNodes {
			if _, attached := visitedNodes[laddr]; !attached {
				orphanedNodes[laddr] = struct{}{}
			}
		}
	}
	if len(visitedNodes)+len(orphanedNodes) != total {
		panic("should not happen")
	}
	dlog.Infof(ctx,
		"... (finished processing %v attached nodes, proceeding to process %v lost nodes, for a total of %v)",
		len(visitedNodes), len(orphanedNodes), len(visitedNodes)+len(orphanedNodes))
	for _, potentialRoot := range maps.SortedKeys(orphanedNodes) {
		walkHandler.Node = func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
			addr := path.Node(-1).ToNodeAddr
			if addr != potentialRoot {
				delete(orphanedNodes, addr)
			}
			visitedNodes[addr] = struct{}{}
			progress()
			return nil
		}
		walkFromNode(ctx, fs, potentialRoot,
			func(err *btrfstree.TreeError) {
				// do nothing
			},
			walkHandler,
		)
	}

	if len(visitedNodes) != total {
		panic("should not happen")
	}

	dlog.Infof(ctx, "... identified %d orphaned nodes and %d bad nodes", len(orphanedNodes), len(badNodes))
	return orphanedNodes, badNodes, nil
}
