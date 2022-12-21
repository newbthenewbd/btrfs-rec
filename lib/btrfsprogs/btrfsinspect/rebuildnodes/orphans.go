// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func listRoots(graph graph.Graph, leaf btrfsvol.LogicalAddr) containers.Set[btrfsvol.LogicalAddr] {
	ret := make(containers.Set[btrfsvol.LogicalAddr])
	_listRoots(ret, graph, leaf)
	return ret
}

func _listRoots(ret containers.Set[btrfsvol.LogicalAddr], graph graph.Graph, leaf btrfsvol.LogicalAddr) {
	kps := graph.EdgesTo[leaf]
	if len(kps) == 0 {
		ret.Insert(leaf)
	}
	for _, kp := range kps {
		_listRoots(ret, graph, kp.FromNode)
	}
}

type crawlStats struct {
	DoneNodes  int
	TotalNodes int
	FoundLeafs int
}

func (s crawlStats) String() string {
	return fmt.Sprintf("... indexing orphaned leafs %v%% (%v/%v); found %d leaf nodes",
		int(100*float64(s.DoneNodes)/float64(s.TotalNodes)),
		s.DoneNodes, s.TotalNodes, s.FoundLeafs)
}

func indexOrphans(ctx context.Context, fs diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock, graph graph.Graph) (
	leaf2orphans map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr],
	err error,
) {

	crawlProgressWriter := textui.NewProgress[crawlStats](ctx, dlog.LogLevelInfo, 1*time.Second)
	progress := func(done int) {
		crawlProgressWriter.Set(crawlStats{
			DoneNodes:  done,
			TotalNodes: len(graph.Nodes),
			FoundLeafs: len(leaf2orphans),
		})
	}
	leaf2orphans = make(map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr])
	for i, node := range maps.SortedKeys(graph.Nodes) {
		progress(i)
		if graph.Nodes[node].Level != 0 {
			continue
		}
		if roots := listRoots(graph, node); !roots.Has(0) {
			leaf2orphans[node] = roots
		}
	}
	progress(len(graph.Nodes))
	crawlProgressWriter.Done()

	return leaf2orphans, nil
}
