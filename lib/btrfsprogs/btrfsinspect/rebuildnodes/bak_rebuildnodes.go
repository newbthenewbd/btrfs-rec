// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

/*
import (
	"context"
	"errors"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
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

		orphanedNodes, badNodes, treeAncestors, err := classifyNodes(ctx, nfs, nodeScanResults)
		if err != nil {
			return nil, err
		}

		uuidMap.considerAncestors(ctx, treeAncestors)

		rebuiltNodes, err := reInitBrokenNodes(ctx, nfs, badNodes)
		if err != nil {
			return nil, err
		}

		if err := reAttachNodes(ctx, nfs, orphanedNodes, rebuiltNodes); err != nil {
			return nil, err
		}

		return rebuiltNodes, nil
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
		},
	})
	return ret, retOK
}
*/
