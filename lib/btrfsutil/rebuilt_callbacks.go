// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type RebuiltForrestCallbacks interface {
	AddedRoot(ctx context.Context, tree btrfsprim.ObjID, root btrfsvol.LogicalAddr)
	LookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool)
	LookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool)
}

type RebuiltForrestExtendedCallbacks interface {
	RebuiltForrestCallbacks
	AddedItem(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key)
}

type noopRebuiltForrestCallbacks struct {
	forrest *RebuiltForrest
}

func (noopRebuiltForrestCallbacks) AddedRoot(context.Context, btrfsprim.ObjID, btrfsvol.LogicalAddr) {
}

func (cb noopRebuiltForrestCallbacks) LookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, _item btrfsitem.Root, ok bool) {
	rootTree, err := cb.forrest.RebuiltTree(ctx, btrfsprim.ROOT_TREE_OBJECTID)
	if err != nil {
		return 0, btrfsitem.Root{}, false
	}
	tgt := btrfsprim.Key{
		ObjectID: tree,
		ItemType: btrfsprim.ROOT_ITEM_KEY,
	}
	itemKey, itemPtr, ok := rootTree.RebuiltAcquireItems(ctx).Search(func(key btrfsprim.Key, _ ItemPtr) int {
		key.Offset = 0
		return tgt.Compare(key)
	})
	rootTree.RebuiltReleaseItems()
	if !ok {
		return 0, btrfsitem.Root{}, false
	}
	item := cb.forrest.readItem(ctx, itemPtr)
	defer item.Body.Free()
	switch itemBody := item.Body.(type) {
	case *btrfsitem.Root:
		return btrfsprim.Generation(itemKey.Offset), *itemBody, true
	case *btrfsitem.Error:
		return 0, btrfsitem.Root{}, false
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

func (cb noopRebuiltForrestCallbacks) LookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
	uuidTree, err := cb.forrest.RebuiltTree(ctx, btrfsprim.UUID_TREE_OBJECTID)
	if err != nil {
		return 0, false
	}
	tgt := btrfsitem.UUIDToKey(uuid)
	itemPtr, ok := uuidTree.RebuiltAcquireItems(ctx).Load(tgt)
	uuidTree.RebuiltReleaseItems()
	if !ok {
		return 0, false
	}
	item := cb.forrest.readItem(ctx, itemPtr)
	defer item.Body.Free()
	switch itemBody := item.Body.(type) {
	case *btrfsitem.UUIDMap:
		return itemBody.ObjID, true
	case *btrfsitem.Error:
		return 0, false
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}