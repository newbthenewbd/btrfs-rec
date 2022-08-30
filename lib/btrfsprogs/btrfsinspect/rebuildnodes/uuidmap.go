// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"encoding/binary"
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

type treeUUIDMap struct {
	ObjID2UUID map[btrfsprim.ObjID]btrfsprim.UUID
	UUID2ObjID map[btrfsprim.UUID]btrfsprim.ObjID
	TreeParent map[btrfsprim.ObjID]btrfsprim.UUID
}

func maybeSet[K, V comparable](name string, m map[K]V, k K, v V) error {
	if other, conflict := m[k]; conflict && other != v {
		return fmt.Errorf("conflict: %s %v can't have both %v and %v", name, k, other, v)
	}
	m[k] = v
	return nil
}

func buildTreeUUIDMap(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (treeUUIDMap, error) {
	dlog.Infof(ctx, "Building table of tree ObjID←→UUID...")

	sb, err := fs.Superblock()
	if err != nil {
		return treeUUIDMap{}, nil
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

	ret := treeUUIDMap{
		ObjID2UUID: make(map[btrfsprim.ObjID]btrfsprim.UUID),
		UUID2ObjID: make(map[btrfsprim.UUID]btrfsprim.ObjID),
		TreeParent: make(map[btrfsprim.ObjID]btrfsprim.UUID),
	}
	treeIDs := make(map[btrfsprim.ObjID]struct{})

	progress()
	for _, devResults := range scanResults {
		for laddr := range devResults.FoundNodes {
			nodeRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
				LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			})
			if err != nil {
				return treeUUIDMap{}, nil
			}
			for _, item := range nodeRef.Data.BodyLeaf {
				switch itemBody := item.Body.(type) {
				case btrfsitem.Root:
					if err := maybeSet("ObjID2UUID", ret.ObjID2UUID, item.Key.ObjectID, itemBody.UUID); err != nil {
						return treeUUIDMap{}, err
					}
					if itemBody.UUID != (btrfsprim.UUID{}) {
						if err := maybeSet("UUID2ObjID", ret.UUID2ObjID, itemBody.UUID, item.Key.ObjectID); err != nil {
							return treeUUIDMap{}, err
						}
					}
					if err := maybeSet("ParentUUID", ret.TreeParent, item.Key.ObjectID, itemBody.ParentUUID); err != nil {
						return treeUUIDMap{}, err
					}
				case btrfsitem.UUIDMap:
					var uuid btrfsprim.UUID
					binary.BigEndian.PutUint64(uuid[:8], uint64(item.Key.ObjectID))
					binary.BigEndian.PutUint64(uuid[8:], uint64(item.Key.Offset))
					if err := maybeSet("ObjID2UUID", ret.ObjID2UUID, itemBody.ObjID, uuid); err != nil {
						return treeUUIDMap{}, err
					}
					if err := maybeSet("UUID2ObjID", ret.UUID2ObjID, uuid, itemBody.ObjID); err != nil {
						return treeUUIDMap{}, err
					}
				}
			}
			treeIDs[nodeRef.Data.Head.Owner] = struct{}{}
			done++
			progress()
		}
	}
	progress()

	missing := make(map[btrfsprim.ObjID]struct{})
	for treeID := range treeIDs {
		if _, ok := ret.ObjID2UUID[treeID]; !ok {
			missing[treeID] = struct{}{}
		}
	}
	if len(missing) > 0 {
		return treeUUIDMap{}, fmt.Errorf("could not find root items for trees %v", maps.SortedKeys(missing))
	}

	dlog.Info(ctx, ".. done building table")
	return ret, nil
}
