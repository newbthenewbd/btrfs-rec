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

type nodeData struct {
	Level      uint8
	Generation btrfsprim.Generation
	Owner      btrfsprim.ObjID
	NumItems   uint32
	MinItem    btrfsprim.Key
	MaxItem    btrfsprim.Key
}

type kpData struct {
	FromTree btrfsprim.ObjID
	FromNode btrfsvol.LogicalAddr
	FromItem int

	ToNode       btrfsvol.LogicalAddr
	ToLevel      uint8
	ToKey        btrfsprim.Key
	ToGeneration btrfsprim.Generation
}

func (kp kpData) String() string {
	return fmt.Sprintf(`{t:%v,n:%v}[%d]->{n:%v,l:%v,g:%v,k:(%d,%v,%d)}`,
		kp.FromTree, kp.FromNode, kp.FromItem,
		kp.ToNode, kp.ToLevel, kp.ToGeneration,
		kp.ToKey.ObjectID,
		kp.ToKey.ItemType,
		kp.ToKey.Offset)
}

type nodeGraph struct {
	Nodes     map[btrfsvol.LogicalAddr]nodeData
	BadNodes  map[btrfsvol.LogicalAddr]error
	EdgesFrom map[btrfsvol.LogicalAddr][]*kpData
	EdgesTo   map[btrfsvol.LogicalAddr][]*kpData
}

func (g nodeGraph) insertEdge(kp kpData) {
	ptr := &kp
	g.EdgesFrom[kp.FromNode] = append(g.EdgesFrom[kp.FromNode], ptr)
	g.EdgesTo[kp.ToNode] = append(g.EdgesTo[kp.ToNode], ptr)
}

func (g nodeGraph) insertTreeRoot(sb btrfstree.Superblock, treeID btrfsprim.ObjID) {
	treeInfo, _ := btrfstree.LookupTreeRoot(nil, sb, treeID)
	g.insertEdge(kpData{
		FromTree:     treeID,
		ToNode:       treeInfo.RootNode,
		ToLevel:      treeInfo.Level,
		ToGeneration: treeInfo.Generation,
	})
}

type scanResult struct {
	uuidMap
	nodeGraph
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
			BadNodes:  make(map[btrfsvol.LogicalAddr]error),
			EdgesFrom: make(map[btrfsvol.LogicalAddr][]*kpData),
			EdgesTo:   make(map[btrfsvol.LogicalAddr][]*kpData),
		},
	}

	// These 4 trees are mentioned directly in the superblock, so
	// they are always seen.
	//
	// 1
	ret.SeenTrees.Insert(btrfsprim.ROOT_TREE_OBJECTID)
	ret.insertTreeRoot(*sb, btrfsprim.ROOT_TREE_OBJECTID)
	// 2
	ret.SeenTrees.Insert(btrfsprim.CHUNK_TREE_OBJECTID)
	ret.insertTreeRoot(*sb, btrfsprim.CHUNK_TREE_OBJECTID)
	// 3
	ret.SeenTrees.Insert(btrfsprim.TREE_LOG_OBJECTID)
	ret.insertTreeRoot(*sb, btrfsprim.TREE_LOG_OBJECTID)
	// 4
	ret.SeenTrees.Insert(btrfsprim.BLOCK_GROUP_TREE_OBJECTID)
	ret.insertTreeRoot(*sb, btrfsprim.BLOCK_GROUP_TREE_OBJECTID)

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
					// graph building
					ret.insertEdge(kpData{
						FromTree:     item.Key.ObjectID,
						ToNode:       itemBody.ByteNr,
						ToLevel:      itemBody.Level,
						ToGeneration: itemBody.Generation,
					})
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
				NumItems:   nodeRef.Data.Head.NumItems,
				MinItem:    func() btrfsprim.Key { k, _ := nodeRef.Data.MinItem(); return k }(),
				MaxItem:    func() btrfsprim.Key { k, _ := nodeRef.Data.MaxItem(); return k }(),
			}
			for i, kp := range nodeRef.Data.BodyInternal {
				ret.insertEdge(kpData{
					FromTree:     nodeRef.Data.Head.Owner,
					FromNode:     laddr,
					FromItem:     i,
					ToNode:       kp.BlockPtr,
					ToLevel:      nodeRef.Data.Head.Level - 1,
					ToKey:        kp.Key,
					ToGeneration: kp.Generation,
				})
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

	dlog.Info(ctx, "... done reading node data")

	dlog.Infof(ctx, "Checking keypointers for dead-ends...")
	total = len(ret.EdgesTo)
	done = 0
	progress()
	for laddr := range ret.EdgesTo {
		if _, ok := ret.Nodes[laddr]; !ok {
			_, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
				LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
			})
			if err == nil {
				return nil, fmt.Errorf("node@%v exists but was not in node scan results", laddr)
			}
			ret.BadNodes[laddr] = err
		}
		done++
		progress()
	}
	dlog.Info(ctx, "... done checking keypointers")

	return ret, nil
}
