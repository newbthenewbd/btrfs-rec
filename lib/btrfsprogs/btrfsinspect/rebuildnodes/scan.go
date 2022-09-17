// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"

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
