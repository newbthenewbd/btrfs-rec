// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	iofs "io/fs"
	"reflect"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

func reInitBrokenNodes(ctx context.Context, fs _FS, nodeScanResults btrfsinspect.ScanDevicesResult, foundRoots map[btrfsvol.LogicalAddr]struct{}) (map[btrfsvol.LogicalAddr]*RebuiltNode, error) {
	dlog.Info(ctx, "Initializing nodes to re-build...")

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
	progress := func(done int) {
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
			progress(len(visitedNodes))
			return nil
		},
		BadNode: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
			node := btrfstree.Node{
				Size:         sb.NodeSize,
				ChecksumType: sb.ChecksumType,
				Head: btrfstree.NodeHeader{
					MetadataUUID:  sb.EffectiveMetadataUUID(),
					Addr:          path.Node(-1).ToNodeAddr,
					ChunkTreeUUID: chunkTreeUUID,
					//Owner:         TBD, // see RebuiltNode.InTrees
					Generation: path.Node(-1).FromGeneration,
					Level:      path.Node(-1).ToNodeLevel,
				},
			}
			min, max := spanOfTreePath(fs, path)
			if other, ok := rebuiltNodes[path.Node(-1).ToNodeAddr]; ok {
				if !reflect.DeepEqual(other.Node, node) {
					dlog.Errorf(ctx, "... mismatch: %v != %v", node, other.Node)
					return err
				}
				if min.Cmp(other.MinKey) > 0 { // if min > other.MinKey {
					other.MinKey = min // take the max of the two
				}
				if max.Cmp(other.MaxKey) < 0 { // if max < other.MaxKey {
					other.MaxKey = max // take the min of the two
				}
				other.InTrees.Insert(path.Node(-1).FromTree)
			} else {
				rebuiltNodes[path.Node(-1).ToNodeAddr] = &RebuiltNode{
					Err:     err.Error(),
					MinKey:  min,
					MaxKey:  max,
					InTrees: containers.Set[btrfsprim.ObjID]{path.Node(-1).FromTree: struct{}{}},
					Node:    node,
				}
			}
			return err
		},
	}

	// We use WalkAllTrees instead of just iterating over
	// nodeScanResults so that we don't need to specifically check
	// if any of the root nodes referenced directly by the
	// superblock are dead.
	progress(len(visitedNodes))
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

	if len(visitedNodes) != total {
		panic("should not happen")
	}

	dlog.Infof(ctx, "... initialized %d nodes", len(rebuiltNodes))
	return rebuiltNodes, nil
}
