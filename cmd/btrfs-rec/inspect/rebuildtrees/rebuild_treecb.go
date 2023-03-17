// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildtrees

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

// AddedItem implements btrfsutil.RebuiltForrestCallbacks.
func (o *rebuilder) AddedItem(_ context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
	o.addedItemQueue.Insert(keyAndTree{
		TreeID: tree,
		Key:    key,
	})
}

// AddedRoot implements btrfsutil.RebuiltForrestCallbacks.
func (o *rebuilder) AddedRoot(_ context.Context, tree btrfsprim.ObjID, _ btrfsvol.LogicalAddr) {
	if retries := o.retryItemQueue[tree]; retries != nil {
		o.addedItemQueue.InsertFrom(retries)
	}
}

// LookupRoot implements btrfsutil.RebuiltForrestCallbacks.
func (o *rebuilder) LookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
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
		o.enqueueRetry(btrfsprim.ROOT_TREE_OBJECTID)
		return 0, btrfsitem.Root{}, false
	}
	itemBody := o.rebuilt.Tree(ctx, wantKey.TreeID).ReadItem(ctx, foundKey)
	defer itemBody.Free()
	switch itemBody := itemBody.(type) {
	case *btrfsitem.Root:
		return btrfsprim.Generation(foundKey.Offset), *itemBody, true
	case *btrfsitem.Error:
		o.FSErr(ctx, fmt.Errorf("error decoding item: %v: %w", foundKey, itemBody.Err))
		return 0, btrfsitem.Root{}, false
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

// LookupUUID implements btrfsutil.RebuiltForrestCallbacks.
func (o *rebuilder) LookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
	wantKey := WantWithTree{
		TreeID: btrfsprim.UUID_TREE_OBJECTID,
		Key:    wantFromKey(btrfsitem.UUIDToKey(uuid)),
	}
	ctx = withWant(ctx, logFieldTreeWant, "resolve parent UUID", wantKey)
	if !o._wantOff(ctx, wantKey) {
		o.enqueueRetry(btrfsprim.UUID_TREE_OBJECTID)
		return 0, false
	}
	itemBody := o.rebuilt.Tree(ctx, wantKey.TreeID).ReadItem(ctx, wantKey.Key.Key())
	defer itemBody.Free()
	switch itemBody := itemBody.(type) {
	case *btrfsitem.UUIDMap:
		return itemBody.ObjID, true
	case *btrfsitem.Error:
		o.FSErr(ctx, fmt.Errorf("error decoding item: %v: %w", wantKey, itemBody.Err))
		return 0, false
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}
