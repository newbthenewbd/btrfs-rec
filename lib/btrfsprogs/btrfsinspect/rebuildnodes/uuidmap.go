// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type uuidMap struct {
	ObjID2UUID map[btrfsprim.ObjID]btrfsprim.UUID
	UUID2ObjID map[btrfsprim.UUID]btrfsprim.ObjID
	TreeParent map[btrfsprim.ObjID]btrfsprim.UUID

	SeenTrees containers.Set[btrfsprim.ObjID]
}

func (m uuidMap) missingRootItems() containers.Set[btrfsprim.ObjID] {
	missing := make(containers.Set[btrfsprim.ObjID])
	for treeID := range m.SeenTrees {
		if _, ok := m.ObjID2UUID[treeID]; !ok && treeID != btrfsprim.ROOT_TREE_OBJECTID {
			missing.Insert(treeID)
			continue
		}
		if _, ok := m.TreeParent[treeID]; !ok && treeID >= btrfsprim.FIRST_FREE_OBJECTID && treeID <= btrfsprim.LAST_FREE_OBJECTID {
			missing.Insert(treeID)
		}
	}
	return missing
}

// ParentTree implements btrfstree.NodeFile.
func (m uuidMap) ParentTree(tree btrfsprim.ObjID) (btrfsprim.ObjID, bool) {
	if tree < btrfsprim.FIRST_FREE_OBJECTID || tree > btrfsprim.LAST_FREE_OBJECTID {
		// no parent
		return 0, true
	}
	parentUUID, ok := m.TreeParent[tree]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	if parentUUID == (btrfsprim.UUID{}) {
		// no parent
		return 0, true
	}
	parentObjID, ok := m.UUID2ObjID[parentUUID]
	if !ok {
		// could not look up parent info
		return 0, false
	}
	return parentObjID, true
}

type fullAncestorLister struct {
	uuidMap       uuidMap
	treeAncestors map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]

	memos map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]
}

func newFullAncestorLister(uuidMap uuidMap, treeAncestors map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]) fullAncestorLister {
	return fullAncestorLister{
		uuidMap:       uuidMap,
		treeAncestors: treeAncestors,
		memos:         make(map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]),
	}
}

func (fa fullAncestorLister) GetFullAncestors(child btrfsprim.ObjID) containers.Set[btrfsprim.ObjID] {
	if memoized, ok := fa.memos[child]; ok {
		if memoized == nil {
			panic(fmt.Errorf("loop involving tree %v", child))
		}
		return memoized
	}
	fa.memos[child] = nil

	ret := make(containers.Set[btrfsprim.ObjID])
	defer func() {
		fa.memos[child] = ret
	}()

	// Try to use '.uuidMap' instead of '.treeAncestors' if possible.
	knownAncestors := make(containers.Set[btrfsprim.ObjID])
	if parent, ok := fa.uuidMap.ParentTree(child); ok {
		if parent == 0 {
			return ret
		}
		knownAncestors.Insert(parent)
	} else {
		knownAncestors.InsertFrom(fa.treeAncestors[child])
	}

	for _, ancestor := range maps.SortedKeys(knownAncestors) {
		ret.Insert(ancestor)
		ret.InsertFrom(fa.GetFullAncestors(ancestor))
	}
	return ret
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
		for ancestor := range fa.GetFullAncestors(missingRoot) {
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
