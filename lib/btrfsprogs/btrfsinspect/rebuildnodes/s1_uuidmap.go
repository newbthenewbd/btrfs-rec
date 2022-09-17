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

func maybeSet[K, V comparable](name string, m map[K]V, k K, v V) error {
	if other, conflict := m[k]; conflict && other != v {
		return fmt.Errorf("conflict: %s %v can't have both %v and %v", name, k, other, v)
	}
	m[k] = v
	return nil
}

type nodeData struct {
	Level      uint8                // 0+1=1
	Generation btrfsprim.Generation // 1+8=9
	Owner      btrfsprim.ObjID      // 9+8=17
	MinItem    btrfsprim.Key        // 17+17=34
	MaxItem    btrfsprim.Key        // 34+17=51
}

type kpData struct {
	From, To   btrfsvol.LogicalAddr // 0+(2*8)=16
	Key        btrfsprim.Key        // 16+17=33
	Generation btrfsprim.Generation // 33+8=41
}

type nodeGraph struct {
	Nodes     map[btrfsvol.LogicalAddr]nodeData
	EdgesFrom map[btrfsvol.LogicalAddr][]*kpData
	EdgesTo   map[btrfsvol.LogicalAddr][]*kpData
}

type scanResult struct {
	uuidMap
	nodeGraph
}

func ScanDevices(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (*scanResult, error) {
	dlog.Infof(ctx, "Building table of ObjID←→UUID...")

	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
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

	ret := &scanResult{
		uuidMap: uuidMap{
			ObjID2UUID: make(map[btrfsprim.ObjID]btrfsprim.UUID),
			UUID2ObjID: make(map[btrfsprim.UUID]btrfsprim.ObjID),
			TreeParent: make(map[btrfsprim.ObjID]btrfsprim.UUID),

			SeenTrees: make(containers.Set[btrfsprim.ObjID]),
		},
		nodeGraph: nodeGraph{
			Nodes:     make(map[btrfsvol.LogicalAddr]nodeData),
			EdgesFrom: make(map[btrfsvol.LogicalAddr][]*kpData),
			EdgesTo:   make(map[btrfsvol.LogicalAddr][]*kpData),
		},
	}

	// These 4 trees are mentioned directly in the superblock, so
	// they are always seen.
	ret.SeenTrees.Insert(btrfsprim.ROOT_TREE_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.CHUNK_TREE_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.TREE_LOG_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.BLOCK_GROUP_TREE_OBJECTID)

	progress()
	for _, devResults := range scanResults {
		for laddr := range devResults.FoundNodes {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
				LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			})
			if err != nil {
				return nil, err
			}

			// UUID map rebuilding
			for _, item := range nodeRef.Data.BodyLeaf {
				switch itemBody := item.Body.(type) {
				case btrfsitem.Root:
					if err := maybeSet("ObjID2UUID", ret.ObjID2UUID, item.Key.ObjectID, itemBody.UUID); err != nil {
						return nil, err
					}
					if itemBody.UUID != (btrfsprim.UUID{}) {
						if err := maybeSet("UUID2ObjID", ret.UUID2ObjID, itemBody.UUID, item.Key.ObjectID); err != nil {
							return nil, err
						}
					}
					if err := maybeSet("ParentUUID", ret.TreeParent, item.Key.ObjectID, itemBody.ParentUUID); err != nil {
						return nil, err
					}
					ret.SeenTrees.Insert(item.Key.ObjectID)
				case btrfsitem.UUIDMap:
					uuid := btrfsitem.KeyToUUID(item.Key)
					if err := maybeSet("ObjID2UUID", ret.ObjID2UUID, itemBody.ObjID, uuid); err != nil {
						return nil, err
					}
					if err := maybeSet("UUID2ObjID", ret.UUID2ObjID, uuid, itemBody.ObjID); err != nil {
						return nil, err
					}
				}
			}
			ret.SeenTrees.Insert(nodeRef.Data.Head.Owner)

			// graph building
			ret.Nodes[laddr] = nodeData{
				Level:      nodeRef.Data.Head.Level,
				Generation: nodeRef.Data.Head.Generation,
				Owner:      nodeRef.Data.Head.Owner,
				MinItem:    func() btrfsprim.Key { k, _ := nodeRef.Data.MinItem(); return k }(),
				MaxItem:    func() btrfsprim.Key { k, _ := nodeRef.Data.MaxItem(); return k }(),
			}
			for _, kp := range nodeRef.Data.BodyInternal {
				dat := &kpData{
					From:       laddr,
					To:         kp.BlockPtr,
					Key:        kp.Key,
					Generation: kp.Generation,
				}
				ret.EdgesFrom[laddr] = append(ret.EdgesFrom[laddr], dat)
				ret.EdgesTo[laddr] = append(ret.EdgesTo[laddr], dat)
			}

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

func buildUUIDMap(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (uuidMap, error) {
	ret, err := ScanDevices(ctx, fs, scanResults)
	if err != nil {
		return uuidMap{}, err
	}
	return ret.uuidMap, nil
}
