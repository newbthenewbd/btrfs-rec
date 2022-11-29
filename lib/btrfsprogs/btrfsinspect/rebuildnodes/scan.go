// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/uuidmap"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type scanResult struct {
	uuidMap   *uuidmap.UUIDMap
	nodeGraph *graph.Graph
}

func ScanDevices(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (*scanResult, error) {
	dlog.Infof(ctx, "Reading node data from FS...")

	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	lastPct := -1
	total := countNodes(scanResults)
	done := 0
	progress := func(done, total int) {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}

	ret := &scanResult{
		uuidMap:   uuidmap.New(),
		nodeGraph: graph.New(*sb),
	}

	progress(done, total)
	for _, devResults := range scanResults {
		for laddr := range devResults.FoundNodes {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
				LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			})
			if err != nil {
				return nil, err
			}

			if err := ret.uuidMap.InsertNode(nodeRef); err != nil {
				return nil, err
			}

			ret.nodeGraph.InsertNode(nodeRef)

			done++
			progress(done, total)
		}
	}

	if done != total {
		panic("should not happen")
	}

	missing := ret.uuidMap.MissingRootItems()
	if len(missing) > 0 {
		dlog.Errorf(ctx, "... could not find root items for %d trees: %v", len(missing), maps.SortedKeys(missing))
	}

	dlog.Info(ctx, "... done reading node data")

	dlog.Infof(ctx, "Checking keypointers for dead-ends...")
	if err := ret.nodeGraph.FinalCheck(fs, *sb, progress); err != nil {
		return nil, err
	}
	dlog.Info(ctx, "... done checking keypointers")

	return ret, nil
}
