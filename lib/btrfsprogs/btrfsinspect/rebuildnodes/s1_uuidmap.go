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

	SeenTrees map[btrfsprim.ObjID]struct{}
}

func (m uuidMap) missingRootItems() map[btrfsprim.ObjID]struct{} {
	missing := make(map[btrfsprim.ObjID]struct{})
	for treeID := range m.SeenTrees {
		if _, ok := m.ObjID2UUID[treeID]; !ok && treeID != btrfsprim.ROOT_TREE_OBJECTID {
			missing[treeID] = struct{}{}
			continue
		}
		if _, ok := m.TreeParent[treeID]; !ok && treeID >= btrfsprim.FIRST_FREE_OBJECTID && treeID <= btrfsprim.LAST_FREE_OBJECTID {
			missing[treeID] = struct{}{}
		}
	}
	return missing
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

		SeenTrees: make(map[btrfsprim.ObjID]struct{}),
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
			ret.SeenTrees[nodeRef.Data.Head.Owner] = struct{}{}
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
