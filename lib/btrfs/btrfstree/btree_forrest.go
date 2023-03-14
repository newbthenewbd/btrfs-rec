// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree

import (
	"context"
	"errors"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

// A TreeRoot is more-or-less a btrfsitem.Root, but simpler; returned by
// LookupTreeRoot.
type TreeRoot struct {
	ID         btrfsprim.ObjID
	RootNode   btrfsvol.LogicalAddr
	Level      uint8
	Generation btrfsprim.Generation
}

// LookupTreeRoot is a utility function to help with implementing the
// 'TreeOperator' interface.
func LookupTreeRoot(_ context.Context, fs TreeOperator, sb Superblock, treeID btrfsprim.ObjID) (*TreeRoot, error) {
	switch treeID {
	case btrfsprim.ROOT_TREE_OBJECTID:
		return &TreeRoot{
			ID:         treeID,
			RootNode:   sb.RootTree,
			Level:      sb.RootLevel,
			Generation: sb.Generation, // XXX: same generation as LOG_TREE?
		}, nil
	case btrfsprim.CHUNK_TREE_OBJECTID:
		return &TreeRoot{
			ID:         treeID,
			RootNode:   sb.ChunkTree,
			Level:      sb.ChunkLevel,
			Generation: sb.ChunkRootGeneration,
		}, nil
	case btrfsprim.TREE_LOG_OBJECTID:
		return &TreeRoot{
			ID:         treeID,
			RootNode:   sb.LogTree,
			Level:      sb.LogLevel,
			Generation: sb.Generation, // XXX: same generation as ROOT_TREE?
		}, nil
	case btrfsprim.BLOCK_GROUP_TREE_OBJECTID:
		return &TreeRoot{
			ID:         treeID,
			RootNode:   sb.BlockGroupRoot,
			Level:      sb.BlockGroupRootLevel,
			Generation: sb.BlockGroupRootGeneration,
		}, nil
	default:
		rootItem, err := fs.TreeSearch(btrfsprim.ROOT_TREE_OBJECTID, SearchRootItem(treeID))
		if err != nil {
			if errors.Is(err, ErrNoItem) {
				err = ErrNoTree
			}
			return nil, fmt.Errorf("tree %s: %w", treeID.Format(btrfsprim.ROOT_TREE_OBJECTID), err)
		}
		switch rootItemBody := rootItem.Body.(type) {
		case *btrfsitem.Root:
			return &TreeRoot{
				ID:         treeID,
				RootNode:   rootItemBody.ByteNr,
				Level:      rootItemBody.Level,
				Generation: rootItemBody.Generation,
			}, nil
		case *btrfsitem.Error:
			return nil, fmt.Errorf("malformed ROOT_ITEM for tree %v: %w", treeID, rootItemBody.Err)
		default:
			panic(fmt.Errorf("should not happen: ROOT_ITEM has unexpected item type: %T", rootItemBody))
		}
	}
}
