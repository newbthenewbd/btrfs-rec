// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

func getTreeAncestors(scanData scanResult) map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID] {
	treeAncestors := make(map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID])

	for laddr, node := range scanData.Nodes {
		if _, ok := treeAncestors[node.Owner]; !ok {
			treeAncestors[node.Owner] = make(containers.Set[btrfsprim.ObjID])
		}
		for _, edge := range scanData.EdgesTo[laddr] {
			treeAncestors[node.Owner].Insert(edge.FromTree)
		}
	}

	return treeAncestors
}
