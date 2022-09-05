// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
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

func (m uuidMap) considerAncestors(ctx context.Context, treeAncestors map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]) {
	if missing := m.missingRootItems(); len(missing) == 0 {
		return
	} else {
		dlog.Infof(ctx, "Rebuilding %d root items...", len(missing))
	}

	var fullAncestors map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID]
	var getFullAncestors func(child btrfsprim.ObjID) containers.Set[btrfsprim.ObjID]
	getFullAncestors = func(child btrfsprim.ObjID) containers.Set[btrfsprim.ObjID] {
		if memoized := fullAncestors[child]; memoized != nil {
			return memoized
		}
		ret := make(containers.Set[btrfsprim.ObjID])
		fullAncestors[child] = ret

		// Try to use 'm' instead of 'treeAncestors' if possible.
		knownAncestors := make(containers.Set[btrfsprim.ObjID])
		if parent, ok := m.ParentTree(child); ok {
			if parent == 0 {
				return ret
			}
			knownAncestors.Insert(parent)
		} else {
			knownAncestors.InsertFrom(treeAncestors[child])
		}

		for _, ancestor := range maps.SortedKeys(knownAncestors) {
			ret.Insert(ancestor)
			ret.InsertFrom(getFullAncestors(ancestor))
		}
		return ret
	}

	fullAncestors = make(map[btrfsprim.ObjID]containers.Set[btrfsprim.ObjID])
	for _, missingRoot := range maps.SortedKeys(m.missingRootItems()) {
		if _, ok := m.TreeParent[missingRoot]; ok {
			// This one is incomplete because it doesn't have a UUID, not
			// because it doesn't have a parent.
			continue
		}
		potentialParents := make(containers.Set[btrfsprim.ObjID])
		potentialParents.InsertFrom(getFullAncestors(missingRoot))
		for ancestor := range getFullAncestors(missingRoot) {
			potentialParents.DeleteFrom(getFullAncestors(ancestor))
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

func maybeSet[K, V comparable](name string, m map[K]V, k K, v V) error {
	if other, conflict := m[k]; conflict && other != v {
		return fmt.Errorf("conflict: %s %v can't have both %v and %v", name, k, other, v)
	}
	m[k] = v
	return nil
}

func buildUUIDMap(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (uuidMap, error) {
	dlog.Infof(ctx, "Building table of ObjID←→UUID...")

	sb, err := fs.Superblock()
	if err != nil {
		return uuidMap{}, nil
	}

	lastPct := -1
	total := countNodes(scanResults)
	done := 0
	progress := func() {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}

	ret := uuidMap{
		ObjID2UUID: make(map[btrfsprim.ObjID]btrfsprim.UUID),
		UUID2ObjID: make(map[btrfsprim.UUID]btrfsprim.ObjID),
		TreeParent: make(map[btrfsprim.ObjID]btrfsprim.UUID),

		SeenTrees: make(containers.Set[btrfsprim.ObjID]),
	}

	progress()
	for _, devResults := range scanResults {
		for laddr := range devResults.FoundNodes {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
				LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			})
			if err != nil {
				return uuidMap{}, nil
			}
			for _, item := range nodeRef.Data.BodyLeaf {
				switch itemBody := item.Body.(type) {
				case btrfsitem.Root:
					if err := maybeSet("ObjID2UUID", ret.ObjID2UUID, item.Key.ObjectID, itemBody.UUID); err != nil {
						return uuidMap{}, err
					}
					if itemBody.UUID != (btrfsprim.UUID{}) {
						if err := maybeSet("UUID2ObjID", ret.UUID2ObjID, itemBody.UUID, item.Key.ObjectID); err != nil {
							return uuidMap{}, err
						}
					}
					if err := maybeSet("ParentUUID", ret.TreeParent, item.Key.ObjectID, itemBody.ParentUUID); err != nil {
						return uuidMap{}, err
					}
				case btrfsitem.UUIDMap:
					uuid := btrfsitem.KeyToUUID(item.Key)
					if err := maybeSet("ObjID2UUID", ret.ObjID2UUID, itemBody.ObjID, uuid); err != nil {
						return uuidMap{}, err
					}
					if err := maybeSet("UUID2ObjID", ret.UUID2ObjID, uuid, itemBody.ObjID); err != nil {
						return uuidMap{}, err
					}
				}
			}
			ret.SeenTrees.Insert(nodeRef.Data.Head.Owner)
			done++
			progress()
		}
	}

	if done != total {
		panic("should not happen")
	}

	missing := ret.missingRootItems()
	if len(missing) > 0 {
		dlog.Errorf(ctx, "... could not find root items for %d trees: %v", len(missing), maps.SortedKeys(missing))
	}

	dlog.Info(ctx, "... done building table")
	return ret, nil
}
