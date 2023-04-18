// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type RebuiltForrestCallbacks interface {
	AddedRoot(ctx context.Context, tree btrfsprim.ObjID, root btrfsvol.LogicalAddr)
	LookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, err error)
	LookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, err error)
}

type RebuiltForrestExtendedCallbacks interface {
	RebuiltForrestCallbacks
	AddedItem(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key)
}

type noopRebuiltForrestCallbacks struct {
	forrest btrfstree.Forrest
}

func (noopRebuiltForrestCallbacks) AddedRoot(context.Context, btrfsprim.ObjID, btrfsvol.LogicalAddr) {
}

func (cb noopRebuiltForrestCallbacks) LookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, _item btrfsitem.Root, err error) {
	rootTree, err := cb.forrest.ForrestLookup(ctx, btrfsprim.ROOT_TREE_OBJECTID)
	if err != nil {
		return 0, btrfsitem.Root{}, err
	}
	item, err := rootTree.TreeSearch(ctx, btrfstree.SearchRootItem(tree))
	if err != nil {
		return 0, btrfsitem.Root{}, err
	}
	defer item.Body.Free()
	switch itemBody := item.Body.(type) {
	case *btrfsitem.Root:
		return btrfsprim.Generation(item.Key.Offset), *itemBody, nil
	case *btrfsitem.Error:
		return 0, btrfsitem.Root{}, itemBody.Err
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

func (cb noopRebuiltForrestCallbacks) LookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, err error) {
	uuidTree, err := cb.forrest.ForrestLookup(ctx, btrfsprim.UUID_TREE_OBJECTID)
	if err != nil {
		return 0, err
	}
	tgt := btrfsitem.UUIDToKey(uuid)
	item, err := uuidTree.TreeLookup(ctx, tgt)
	if err != nil {
		return 0, err
	}
	defer item.Body.Free()
	switch itemBody := item.Body.(type) {
	case *btrfsitem.UUIDMap:
		return itemBody.ObjID, nil
	case *btrfsitem.Error:
		return 0, itemBody.Err
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}
