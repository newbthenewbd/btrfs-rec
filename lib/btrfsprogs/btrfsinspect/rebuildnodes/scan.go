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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes/graph"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type scanResult struct {
	uuidMap
	nodeGraph *graph.Graph
}

func ScanDevices(ctx context.Context, fs *btrfs.FS, scanResults btrfsinspect.ScanDevicesResult) (*scanResult, error) {
	dlog.Infof(ctx, "Reading node data from FS...")

	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	lastPct := -1
	total := countNodes(scanResults)
	done := 0
	progress := func(done, total int) {
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
		nodeGraph: graph.New(*sb),
	}

	// These 4 trees are mentioned directly in the superblock, so
	// they are always seen.
	ret.SeenTrees.Insert(btrfsprim.ROOT_TREE_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.CHUNK_TREE_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.TREE_LOG_OBJECTID)
	ret.SeenTrees.Insert(btrfsprim.BLOCK_GROUP_TREE_OBJECTID)

	progress(done, total)
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
			ret.nodeGraph.InsertNode(nodeRef)

			done++
			progress(done, total)
		}
	}

	if done != total {
		panic("should not happen")
	}

	missing := ret.missingRootItems()
	if len(missing) > 0 {
		dlog.Errorf(ctx, "... could not find root items for %d trees: %v", len(missing), maps.SortedKeys(missing))
	}

	dlog.Info(ctx, "... done reading node data")

	dlog.Infof(ctx, "Checking keypointers for dead-ends...")
	if err := ret.nodeGraph.FinalCheck(fs, *sb, progress); err != nil {
		return nil, err
	}
	dlog.Info(ctx, "... done checking keypointers")

	return ret, nil
}
