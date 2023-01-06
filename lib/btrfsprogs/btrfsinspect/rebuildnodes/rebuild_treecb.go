// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
)

func (o *rebuilder) cbAddedItem(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
	if handleWouldBeNoOp(key.ItemType) {
		return
	}
	o.itemQueue.Insert(keyAndTree{
		TreeID: tree,
		Key:    key,
	})
}

func (o *rebuilder) cbLookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
	wantKey := WantWithTree{
		TreeID: btrfsprim.ROOT_TREE_OBJECTID,
		Key: Want{
			ObjectID:   tree,
			ItemType:   btrfsitem.ROOT_ITEM_KEY,
			OffsetType: offsetAny,
		},
	}
	ctx = withWant(ctx, logFieldTreeWant, "tree Root", wantKey)
	foundKey, ok := o._want(ctx, wantKey)
	if !ok {
		o.enqueueRetry()
		return 0, btrfsitem.Root{}, false
	}
	switch itemBody := o.rebuilt.Tree(ctx, wantKey.TreeID).ReadItem(ctx, foundKey).(type) {
	case *btrfsitem.Root:
		return btrfsprim.Generation(foundKey.Offset), *itemBody, true
	case *btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: %v: %w", foundKey, itemBody.Err))
		return 0, btrfsitem.Root{}, false
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

func (o *rebuilder) cbLookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
	wantKey := WantWithTree{
		TreeID: btrfsprim.UUID_TREE_OBJECTID,
		Key:    wantFromKey(btrfsitem.UUIDToKey(uuid)),
	}
	ctx = withWant(ctx, logFieldTreeWant, "resolve parent UUID", wantKey)
	if !o._wantOff(ctx, wantKey) {
		o.enqueueRetry()
		return 0, false
	}
	switch itemBody := o.rebuilt.Tree(ctx, wantKey.TreeID).ReadItem(ctx, wantKey.Key.Key()).(type) {
	case *btrfsitem.UUIDMap:
		return itemBody.ObjID, true
	case *btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: %v: %w", wantKey, itemBody.Err))
		return 0, false
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}
