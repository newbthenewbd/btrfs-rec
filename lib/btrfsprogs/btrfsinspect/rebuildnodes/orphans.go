// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
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

func walk(graph graph.Graph, root btrfsvol.LogicalAddr, fn func(btrfsvol.LogicalAddr) bool) {
	if _, ok := graph.Nodes[root]; !ok {
		return
	}
	if !fn(root) {
		return
	}
	for _, kp := range graph.EdgesFrom[root] {
		walk(graph, kp.ToNode, fn)
	}
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

func indexOrphans(fs diskio.File[btrfsvol.LogicalAddr], sb btrfstree.Superblock, graph graph.Graph) (
	orphans containers.Set[btrfsvol.LogicalAddr],
	leaf2orphans map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr],
	key2leaf *containers.SortedMap[keyAndTree, btrfsvol.LogicalAddr],
	err error,
) {
	orphans = make(containers.Set[btrfsvol.LogicalAddr])
	for node := range graph.Nodes {
		if len(graph.EdgesTo[node]) == 0 {
			orphans.Insert(node)
		}
	}

	leaf2orphans = make(map[btrfsvol.LogicalAddr]containers.Set[btrfsvol.LogicalAddr])
	visited := make(containers.Set[btrfsvol.LogicalAddr])
	for orphan := range orphans {
		walk(graph, orphan, func(node btrfsvol.LogicalAddr) bool {
			if visited.Has(node) {
				return false
			}
			visited.Insert(node)
			if graph.Nodes[node].Level == 0 {
				if roots := listRoots(graph, node); !roots.Has(0) {
					leaf2orphans[node] = roots
				}
			}
			return true
		})
	}

	key2leaf = new(containers.SortedMap[keyAndTree, btrfsvol.LogicalAddr])
	for laddr := range leaf2orphans {
		nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, sb, laddr, btrfstree.NodeExpectations{
			LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			Level: containers.Optional[uint8]{OK: true, Val: 0},
		})
		if err != nil {
			return nil, nil, nil, err
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
	return orphans, leaf2orphans, key2leaf, nil
}
