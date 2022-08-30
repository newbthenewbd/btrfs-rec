// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	iofs "io/fs"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func reInitBrokenNodes(ctx context.Context, fs _FS, nodeScanResults btrfsinspect.ScanDevicesResult, foundRoots map[btrfsvol.LogicalAddr]struct{}) (map[btrfsvol.LogicalAddr]*RebuiltNode, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	chunkTreeUUID, ok := getChunkTreeUUID(ctx, fs)
	if !ok {
		return nil, fmt.Errorf("could not look up chunk tree UUID")
	}

	lastPct := -1
	total := countNodes(nodeScanResults)
	done := 0
	progress := func() {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}

	rebuiltNodes := make(map[btrfsvol.LogicalAddr]*RebuiltNode)
	visitedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	walkHandler := btrfstree.TreeWalkHandler{
		Node: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
			addr := path.Node(-1).ToNodeAddr
			if _, alreadyVisited := visitedNodes[addr]; alreadyVisited {
				// Can happen because of COW subvolumes;
				// this is really a DAG not a tree.
				return iofs.SkipDir
			}
			visitedNodes[addr] = struct{}{}
			done++
			progress()
			return nil
		},
		BadNode: func(path btrfstree.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
			min, max := spanOfTreePath(fs, path)
			rebuiltNodes[path.Node(-1).ToNodeAddr] = &RebuiltNode{
				Err:    err,
				MinKey: min,
				MaxKey: max,
				Node: btrfstree.Node{
					Head: btrfstree.NodeHeader{
						MetadataUUID:  sb.EffectiveMetadataUUID(),
						Addr:          path.Node(-1).ToNodeAddr,
						ChunkTreeUUID: chunkTreeUUID,
						Owner:         path.Node(-1).FromTree,
						Generation:    path.Node(-1).FromGeneration,
						Level:         path.Node(-1).ToNodeLevel,
					},
				},
			}
			return err
		},
	}

	// We use WalkAllTrees instead of just iterating over
	// nodeScanResults so that we don't need to specifically check
	// if any of the root nodes referenced directly by the
	// superblock are dead.
	progress()
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
		},
		TreeWalkHandler: walkHandler,
	})
	for foundRoot := range foundRoots {
		walkFromNode(ctx, fs, foundRoot,
			func(err *btrfstree.TreeError) {
				// do nothing
			},
			walkHandler)
	}
	progress()

	return rebuiltNodes, nil
}
