// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	iofs "io/fs"
	"math"
	"sort"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func RebuildNodes(ctx context.Context, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) (map[btrfsvol.LogicalAddr]*RebuiltNode, error) {
	uuidMap, err := buildUUIDMap(ctx, fs, nodeScanResults)
	if err != nil {
		return nil, err
	}

	nfs := &RebuiltTrees{
		inner:   fs,
		uuidMap: uuidMap,
	}

	dlog.Info(ctx, "Identifying lost+found nodes...")
	foundRoots, err := lostAndFoundNodes(ctx, nfs, nodeScanResults)
	if err != nil {
		return nil, err
	}
	dlog.Infof(ctx, "... identified %d lost+found nodes", len(foundRoots))

	dlog.Info(ctx, "Initializing nodes to re-build...")
	rebuiltNodes, err := reInitBrokenNodes(ctx, nfs, nodeScanResults, foundRoots)
	if err != nil {
		return nil, err
	}
	dlog.Infof(ctx, "Initialized %d nodes", len(rebuiltNodes))

	dlog.Info(ctx, "Attaching lost+found nodes to rebuilt nodes...")
	if err := reAttachNodes(ctx, nfs, foundRoots, rebuiltNodes); err != nil {
		return nil, err
	}
	dlog.Info(ctx, "... done attaching")

	return rebuiltNodes, nil
}

var maxKey = btrfsprim.Key{
	ObjectID: math.MaxUint64,
	ItemType: math.MaxUint8,
	Offset:   math.MaxUint64,
}

func keyMm(key btrfsprim.Key) btrfsprim.Key {
	switch {
	case key.Offset > 0:
		key.Offset--
	case key.ItemType > 0:
		key.ItemType--
	case key.ObjectID > 0:
		key.ObjectID--
	}
	return key
}

func spanOfTreePath(fs _FS, path btrfstree.TreePath) (btrfsprim.Key, btrfsprim.Key) {
	// tree root error
	if len(path) == 0 {
		return btrfsprim.Key{}, maxKey
	}

	// item error
	if path.Node(-1).ToNodeAddr == 0 {
		// If we got an item error, then the node is readable
		node, _ := fs.ReadNode(path.Parent())
		key := node.Data.BodyLeaf[path.Node(-1).FromItemIdx].Key
		return key, key
	}

	// node error
	//
	// assume that path.Node(-1).ToNodeAddr is not readable, but that path.Node(-2).ToNodeAddr is.
	if len(path) == 1 {
		return btrfsprim.Key{}, maxKey
	}
	parentNode, _ := fs.ReadNode(path.Parent())
	low := parentNode.Data.BodyInternal[path.Node(-1).FromItemIdx].Key
	var high btrfsprim.Key
	if path.Node(-1).FromItemIdx+1 < len(parentNode.Data.BodyInternal) {
		high = keyMm(parentNode.Data.BodyInternal[path.Node(-1).FromItemIdx+1].Key)
	} else {
		parentPath := path.Parent().DeepCopy()
		_, high = spanOfTreePath(fs, parentPath)
	}
	return low, high
}

func walkFromNode(ctx context.Context, fs _FS, nodeAddr btrfsvol.LogicalAddr, errHandle func(*btrfstree.TreeError), cbs btrfstree.TreeWalkHandler) {
	sb, _ := fs.Superblock()
	nodeRef, _ := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, nodeAddr, btrfstree.NodeExpectations{
		LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: nodeAddr},
	})
	if nodeRef == nil {
		return
	}
	treeInfo := btrfstree.TreeRoot{
		TreeID:     nodeRef.Data.Head.Owner,
		RootNode:   nodeAddr,
		Level:      nodeRef.Data.Head.Level,
		Generation: nodeRef.Data.Head.Generation,
	}
	btrfstree.TreeOperatorImpl{NodeSource: fs}.RawTreeWalk(ctx, treeInfo, errHandle, cbs)
}

func countNodes(nodeScanResults btrfsinspect.ScanDevicesResult) int {
	var cnt int
	for _, devResults := range nodeScanResults {
		cnt += len(devResults.FoundNodes)
	}
	return cnt
}

func getChunkTreeUUID(ctx context.Context, fs _FS) (btrfsprim.UUID, bool) {
	ctx, cancel := context.WithCancel(ctx)
	var ret btrfsprim.UUID
	var retOK bool
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		TreeWalkHandler: btrfstree.TreeWalkHandler{
			Node: func(path btrfstree.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
				ret = node.Data.Head.ChunkTreeUUID
				retOK = true
				cancel()
				return nil
			},
		},
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
			dlog.Errorf(ctx, "dbg err: %v", err)
		},
	})
	return ret, retOK
}

type RebuiltNode struct {
	Err            error
	MinKey, MaxKey btrfsprim.Key
	btrfstree.Node
}

func reInitBrokenNodes(ctx context.Context, fs _FS, nodeScanResults btrfsinspect.ScanDevicesResult, foundRoots map[btrfsvol.LogicalAddr]struct{}) (map[btrfsvol.LogicalAddr]*RebuiltNode, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}

	chunkTreeUUID, ok := getChunkTreeUUID(ctx, fs)
	if !ok {
		return nil, fmt.Errorf("could not look up chunk tree UUID")
	}

	lastPct := -1
	total := countNodes(nodeScanResults)
	done := 0
	progress := func() {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastPct || done == total {
			dlog.Infof(ctx, "... %v%% (%v/%v)",
				pct, done, total)
			lastPct = pct
		}
	}

	rebuiltNodes := make(map[btrfsvol.LogicalAddr]*RebuiltNode)
	visitedNodes := make(map[btrfsvol.LogicalAddr]struct{})
	walkHandler := btrfstree.TreeWalkHandler{
		Node: func(path btrfstree.TreePath, _ *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
			addr := path.Node(-1).ToNodeAddr
			if _, alreadyVisited := visitedNodes[addr]; alreadyVisited {
				// Can happen because of COW subvolumes;
				// this is really a DAG not a tree.
				return iofs.SkipDir
			}
			visitedNodes[addr] = struct{}{}
			done++
			progress()
			return nil
		},
		BadNode: func(path btrfstree.TreePath, node *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node], err error) error {
			min, max := spanOfTreePath(fs, path)
			rebuiltNodes[path.Node(-1).ToNodeAddr] = &RebuiltNode{
				Err:    err,
				MinKey: min,
				MaxKey: max,
				Node: btrfstree.Node{
					Head: btrfstree.NodeHeader{
						MetadataUUID:  sb.EffectiveMetadataUUID(),
						Addr:          path.Node(-1).ToNodeAddr,
						ChunkTreeUUID: chunkTreeUUID,
						Owner:         path.Node(-1).FromTree,
						Generation:    path.Node(-1).FromGeneration,
						Level:         path.Node(-1).ToNodeLevel,
					},
				},
			}
			return err
		},
	}

	// We use WalkAllTrees instead of just iterating over
	// nodeScanResults so that we don't need to specifically check
	// if any of the root nodes referenced directly by the
	// superblock are dead.
	progress()
	btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
		Err: func(err *btrfsutil.WalkError) {
			// do nothing
		},
		TreeWalkHandler: walkHandler,
	})
	for foundRoot := range foundRoots {
		walkFromNode(ctx, fs, foundRoot,
			func(err *btrfstree.TreeError) {
				// do nothing
			},
			walkHandler)
	}
	progress()

	return rebuiltNodes, nil
}

func reAttachNodes(ctx context.Context, fs _FS, foundRoots map[btrfsvol.LogicalAddr]struct{}, rebuiltNodes map[btrfsvol.LogicalAddr]*RebuiltNode) error {
	// Index 'rebuiltNodes' for fast lookups.
	gaps := make(map[btrfsprim.ObjID]map[uint8][]*RebuiltNode)
	maxLevel := make(map[btrfsprim.ObjID]uint8)
	for _, node := range rebuiltNodes {
		maxLevel[node.Head.Owner] = slices.Max(maxLevel[node.Head.Owner], node.Head.Level)

		if gaps[node.Head.Owner] == nil {
			gaps[node.Head.Owner] = make(map[uint8][]*RebuiltNode)
		}
		gaps[node.Head.Owner][node.Head.Level] = append(gaps[node.Head.Owner][node.Head.Level], node)
	}
	for _, byTreeID := range gaps {
		for _, slice := range byTreeID {
			sort.Slice(slice, func(i, j int) bool {
				return slice[i].MinKey.Cmp(slice[j].MinKey) < 0
			})
		}
	}

	// Attach foundRoots to the gaps.
	sb, _ := fs.Superblock()
	for foundLAddr := range foundRoots {
		foundRef, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, foundLAddr, btrfstree.NodeExpectations{
			LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: foundLAddr},
		})
		if foundRef == nil {
			return err
		}
		foundMinKey, ok := foundRef.Data.MinItem()
		if !ok {
			continue
		}
		foundMaxKey, ok := foundRef.Data.MaxItem()
		if !ok {
			continue
		}
		treeGaps := gaps[foundRef.Data.Head.Owner]
		var attached bool
		for level := foundRef.Data.Head.Level + 1; treeGaps != nil && level <= maxLevel[foundRef.Data.Head.Owner] && !attached; level++ {
			parentGen, ok := treeGaps[level]
			if !ok {
				continue
			}
			parentIdx, ok := slices.Search(parentGen, func(parent *RebuiltNode) int {
				switch {
				case foundMinKey.Cmp(parent.MinKey) < 0:
					// 'parent' is too far right
					return -1
				case foundMaxKey.Cmp(parent.MaxKey) > 0:
					// 'parent' is too far left
					return 1
				default:
					// just right
					return 0
				}
			})
			if !ok {
				continue
			}
			parent := parentGen[parentIdx]
			parent.BodyInternal = append(parent.BodyInternal, btrfstree.KeyPointer{
				Key:        foundMinKey,
				BlockPtr:   foundLAddr,
				Generation: foundRef.Data.Head.Generation,
			})
			attached = true
		}
		if !attached {
			dlog.Errorf(ctx, "could not find a broken node to attach node to reattach node@%v to",
				foundRef.Addr)
		}
	}

	return nil
}
