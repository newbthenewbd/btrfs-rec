// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"

	//"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

func getTreeAncestors(ctx context.Context, scanData scanResult) map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID] {
	treeAncestors := make(map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID])

	for laddr, node := range scanData.Nodes {
		if _, ok := treeAncestors[node.Owner]; !ok {
			treeAncestors[node.Owner] = make(containers.Set[btrfsprim.ObjID])
		}
		for _, edge := range scanData.EdgesTo[laddr] {
			if edge.FromTree != node.Owner {
				if err := checkNodeExpectations(*edge, node); err != nil {
					//dlog.Errorf(ctx, "... ignoring keypointer %v because %v", edge.String(), err)
				} else {
					treeAncestors[node.Owner].Insert(edge.FromTree)
				}
			}
		}
	}

	return treeAncestors
}
