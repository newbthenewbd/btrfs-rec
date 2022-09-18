// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

/*
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

func (m uuidMap) considerAncestors(ctx context.Context, treeAncestors map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]) {
	if missing := m.missingRootItems(); len(missing) == 0 {
		return
	} else {
		dlog.Infof(ctx, "Rebuilding %d root items...", len(missing))
	}

	fa := newFullAncestorLister(m, treeAncestors)

	for _, missingRoot := range maps.SortedKeys(m.missingRootItems()) {
		if _, ok := m.TreeParent[missingRoot]; ok {
			// This one is incomplete because it doesn't have a UUID, not
			// because it doesn't have a parent.
			continue
		}
		potentialParents := make(containers.Set[btrfsprim.ObjID])
		potentialParents.InsertFrom(fa.GetFullAncestors(missingRoot))
		for _, ancestor := range maps.SortedKeys(fa.GetFullAncestors(missingRoot)) {
			potentialParents.DeleteFrom(fa.GetFullAncestors(ancestor))
		}
		if len(potentialParents) == 1 {
			parent := potentialParents.TakeOne()
			dlog.Infof(ctx, "... the parent of %v is %v", missingRoot, parent)
			parentUUID, ok := m.ObjID2UUID[parent]
			if !ok {
				dlog.Errorf(ctx, "... but can't synthesize a root item because UUID of %v is unknown", parent)
				continue
			}
			m.TreeParent[missingRoot] = parentUUID
		}
	}

	if missing := m.missingRootItems(); len(missing) > 0 {
		dlog.Errorf(ctx, "... could not rebuild root items for %d trees: %v", len(missing), maps.SortedKeys(missing))
	} else {
		dlog.Info(ctx, "... rebuilt all root items")
	}
}
*/
