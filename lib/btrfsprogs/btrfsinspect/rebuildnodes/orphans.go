// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func listRoots(graph graph.Graph, leaf btrfsvol.LogicalAddr) containers.Set[btrfsvol.LogicalAddr] {
	kps := graph.EdgesTo[leaf]
	if len(kps) == 0 {
		return containers.NewSet(leaf)
	}
	ret := make(containers.Set[btrfsvol.LogicalAddr])
	for _, kp := range kps {
		ret.InsertFrom(listRoots(graph, kp.FromNode))
	}
	return ret
}

type keyAndTree struct {
	btrfsprim.Key
	TreeID btrfsprim.ObjID
}

func (a keyAndTree) Cmp(b keyAndTree) int {
	if d := a.Key.Cmp(b.Key); d != 0 {
		return d
	}
	return containers.NativeCmp(a.TreeID, b.TreeID)
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

type readStats struct {
	DoneLeafNodes  int
	TotalLeafNodes int
}

func (s readStats) String() string {
	return fmt.Sprintf("... reading leafs %v%% (%v/%v)",
		int(100*float64(s.DoneLeafNodes)/float64(s.TotalLeafNodes)),
		s.DoneLeafNodes, s.TotalLeafNodes)
}

func indexOrphans(ctx context.Context, fs diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock, graph graph.Graph) (
	leaf2orphans map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr],
	key2leaf *containers.SortedMap[keyAndTree, btrfsvol.LogicalAddr],
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

	key2leaf = new(containers.SortedMap[keyAndTree, btrfsvol.LogicalAddr])
	readProgressWriter := textui.NewProgress[readStats](ctx, dlog.LogLevelInfo, 1*time.Second)
	progress = func(done int) {
		readProgressWriter.Set(readStats{
			DoneLeafNodes:  done,
			TotalLeafNodes: len(leaf2orphans),
		})
	}
	for i, laddr := range maps.SortedKeys(leaf2orphans) {
		progress(i)
		nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, sb, laddr, btrfstree.NodeExpectations{
			LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			Level: containers.Optional[uint8]{OK: true, Val: 0},
		})
		if err != nil {
			return nil, nil, err
		}

		for _, item := range nodeRef.Data.BodyLeaf {
			k := keyAndTree{
				Key:    item.Key,
				TreeID: nodeRef.Data.Head.Owner,
			}
			if cur, ok := key2leaf.Load(k); !ok || graph.Nodes[cur].Generation < nodeRef.Data.Head.Generation {
				key2leaf.Store(k, laddr)
			}
		}
	}
	progress(len(leaf2orphans))
	readProgressWriter.Done()

	return leaf2orphans, key2leaf, nil
}
