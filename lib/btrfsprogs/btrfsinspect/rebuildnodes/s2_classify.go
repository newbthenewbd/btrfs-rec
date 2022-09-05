// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"errors"
	iofs "io/fs"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
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
//  3. tree ancestors thing
func classifyNodes(ctx context.Context, fs _FS, scanResults btrfsinspect.ScanDevicesResult) (
	orphanedNodes containers.Set[btrfsvol.LogicalAddr],
	badNodes []badNode,
	treeAncestors map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID],
	err error,
) {
	dlog.Info(ctx, "Walking trees to identify orphan and broken nodes...")

	lastPct := -1
	total := countNodes(scanResults)
	visitedNodes := make(containers.Set[btrfsvol.LogicalAddr])
	progress := func() {
		done := len(visitedNodes)
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}

	treeAncestors = make(map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID])

	var potentialRoot btrfsvol.LogicalAddr // zero for non-lost nodes, non-zero for lost nodes
	nodeHandler := func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
		if err != nil && (errors.Is(err, btrfstree.ErrNotANode) || errors.As(err, new(*btrfstree.IOError))) {
			badNodes = append(badNodes, badNode{
				Err:  err.Error(),
				Path: path.DeepCopy(),
			})
			return err
		}
		addr := path.Node(-1).ToNodeAddr
		visitedNodes.Insert(addr)
		if potentialRoot != 0 {
			// lost node
			if addr != potentialRoot {
				delete(orphanedNodes, addr)
			}
		}
		if err == nil && nodeRef.Data.Head.Owner != path.Node(-1).FromTree {
			if treeAncestors[path.Node(-1).FromTree] == nil {
				treeAncestors[path.Node(-1).FromTree] = make(containers.Set[btrfsprim.ObjID])
			}
			treeAncestors[path.Node(-1).FromTree].Insert(nodeRef.Data.Head.Owner)
		}
		progress()
		return nil
	}

	walkHandler := btrfstree.TreeWalkHandler{
		PreNode: func(path btrfstree.TreePath) error {
			addr := path.Node(-1).ToNodeAddr
			if visitedNodes.Has(addr) {
				// Can happen because of COW subvolumes;
				// this is really a DAG not a tree.
				return iofs.SkipDir
			}
			return nil
		},
		Node: func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
			return nodeHandler(path, nodeRef, nil)
		},
		BadNode: func(path btrfstree.TreePath, nodeRef *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
			return nodeHandler(path, nodeRef, err)
		},
	}

	progress()
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: walkHandler,
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
		},
	})

	// Start with 'orphanedRoots' as a complete set of all orphaned nodes, and then delete
	// non-root entries from it.
	orphanedNodes = make(containers.Set[btrfsvol.LogicalAddr])
	for _, devResults := range scanResults {
		for laddr := range devResults.FoundNodes {
			if !visitedNodes.Has(laddr) {
				orphanedNodes.Insert(laddr)
			}
		}
	}
	if len(visitedNodes)+len(orphanedNodes) != total {
		panic("should not happen")
	}
	dlog.Infof(ctx,
		"... (finished processing %v attached nodes, proceeding to process %v lost nodes, for a total of %v)",
		len(visitedNodes), len(orphanedNodes), len(visitedNodes)+len(orphanedNodes))
	for _, potentialRoot = range maps.SortedKeys(orphanedNodes) {
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
	return orphanedNodes, badNodes, treeAncestors, nil
}
