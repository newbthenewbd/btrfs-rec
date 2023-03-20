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
				err = fmt.Errorf("%w: %s", ErrNoTree, err)
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

type TreeOperatorImpl struct {
	NodeSource
}

func (fs TreeOperatorImpl) RawTree(ctx context.Context, treeID btrfsprim.ObjID) (*RawTree, error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, err
	}
	rootInfo, err := LookupTreeRoot(ctx, fs, *sb, treeID)
	if err != nil {
		return nil, err
	}
	return &RawTree{
		Forrest:  fs,
		TreeRoot: *rootInfo,
	}, nil
}

// TreeWalk implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeWalk(ctx context.Context, treeID btrfsprim.ObjID, errHandle func(*TreeError), cbs TreeWalkHandler) {
	tree, err := fs.RawTree(ctx, treeID)
	if err != nil {
		errHandle(&TreeError{Path: Path{{FromTree: treeID, ToMaxKey: btrfsprim.MaxKey}}, Err: err})
		return
	}
	tree.TreeWalk(ctx, cbs)
}

// TreeLookup implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeLookup(treeID btrfsprim.ObjID, key btrfsprim.Key) (Item, error) {
	ctx := context.TODO()
	tree, err := fs.RawTree(ctx, treeID)
	if err != nil {
		return Item{}, err
	}
	return tree.TreeLookup(ctx, key)
}

// TreeSearch implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeSearch(treeID btrfsprim.ObjID, searcher TreeSearcher) (Item, error) {
	ctx := context.TODO()
	tree, err := fs.RawTree(ctx, treeID)
	if err != nil {
		return Item{}, err
	}
	return tree.TreeSearch(ctx, searcher)
}

// TreeSearchAll implements the 'TreeOperator' interface.
func (fs TreeOperatorImpl) TreeSearchAll(treeID btrfsprim.ObjID, searcher TreeSearcher) ([]Item, error) {
	ctx := context.TODO()
	tree, err := fs.RawTree(ctx, treeID)
	if err != nil {
		return nil, err
	}

	var ret []Item
	err = tree.TreeSubrange(ctx, 1, searcher, func(item Item) bool {
		item.Body = item.Body.CloneItem()
		ret = append(ret, item)
		return true
	})

	return ret, err
}
