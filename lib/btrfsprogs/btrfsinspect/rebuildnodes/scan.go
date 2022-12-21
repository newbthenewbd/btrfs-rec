// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/uuidmap"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type scanResult struct {
	uuidMap   *uuidmap.UUIDMap
	nodeGraph *graph.Graph
}

type scanStats struct {
	N, D int
}

func (s scanStats) String() string {
	return fmt.Sprintf("... %v%% (%v/%v)",
		int(100*float64(s.N)/float64(s.D)),
		s.N, s.D)
}

func ScanDevices(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (*scanResult, error) {
	dlog.Infof(ctx, "Reading node data from FS...")

	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	total := countNodes(scanResults)
	done := 0
	progressWriter := textui.NewProgress[scanStats](ctx, dlog.LogLevelInfo, 1*time.Second)
	progress := func(done, total int) {
		progressWriter.Set(scanStats{N: done, D: total})
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
	progressWriter.Done()

	missing := ret.uuidMap.MissingRootItems()
	if len(missing) > 0 {
		dlog.Errorf(ctx, "... could not find root items for %d trees: %v", len(missing), maps.SortedKeys(missing))
	}

	dlog.Info(ctx, "... done reading node data")

	progressWriter = textui.NewProgress[scanStats](ctx, dlog.LogLevelInfo, 1*time.Second)
	dlog.Infof(ctx, "Checking keypointers for dead-ends...")
	if err := ret.nodeGraph.FinalCheck(fs, *sb, progress); err != nil {
		return nil, err
	}
	progressWriter.Done()
	dlog.Info(ctx, "... done checking keypointers")

	return ret, nil
}
