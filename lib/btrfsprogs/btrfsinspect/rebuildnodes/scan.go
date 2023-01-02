// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/keyio"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func ScanDevices(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (graph.Graph, *keyio.Handle, error) {
	dlog.Infof(ctx, "Reading node data from FS...")

	sb, err := fs.Superblock()
	if err != nil {
		return graph.Graph{}, nil, err
	}

	var stats textui.Portion[int]
	stats.D = countNodes(scanResults)
	progressWriter := textui.NewProgress[textui.Portion[int]](
		dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.read.substep", "read-nodes"),
		dlog.LogLevelInfo, textui.Tunable(1*time.Second))

	nodeGraph := graph.New(*sb)
	keyIO := keyio.NewHandle(fs, *sb)

	progressWriter.Set(stats)
	for _, devResults := range scanResults {
		for laddr := range devResults.FoundNodes {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
				LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			})
			if err != nil {
				return graph.Graph{}, nil, err
			}

			nodeGraph.InsertNode(nodeRef)
			keyIO.InsertNode(nodeRef)

			stats.N++
			progressWriter.Set(stats)
		}
	}
	if stats.N != stats.D {
		panic("should not happen")
	}
	progressWriter.Done()
	dlog.Info(ctx, "... done reading node data")

	if err := nodeGraph.FinalCheck(ctx, fs, *sb); err != nil {
		return graph.Graph{}, nil, err
	}
	keyIO.SetGraph(*nodeGraph)

	return *nodeGraph, keyIO, nil
}
