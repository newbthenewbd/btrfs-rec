// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildtrees

import (
	"context"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
)

type forrestCallbacks struct {
	*rebuilder
}

var _ btrfsutil.RebuiltForrestExtendedCallbacks = forrestCallbacks{}

// AddedItem implements btrfsutil.RebuiltForrestExtendedCallbacks.
func (o forrestCallbacks) AddedItem(_ context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
	o.addedItemQueue.Insert(keyAndTree{
		TreeID: tree,
		Key:    key,
	})
}

// AddedRoot implements btrfsutil.RebuiltForrestCallbacks.
func (o forrestCallbacks) AddedRoot(_ context.Context, tree btrfsprim.ObjID, _ btrfsvol.LogicalAddr) {
	if retries := o.retryItemQueue[tree]; retries != nil {
		o.addedItemQueue.InsertFrom(retries)
	}
}

// LookupRoot implements btrfsutil.RebuiltForrestCallbacks.
func (o forrestCallbacks) LookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, _item btrfsitem.Root, err error) {
	wantKey := wantWithTree{
		TreeID: btrfsprim.ROOT_TREE_OBJECTID,
		Key: want{
			ObjectID:   tree,
			ItemType:   btrfsitem.ROOT_ITEM_KEY,
			OffsetType: offsetAny,
		},
	}
	ctx = withWant(ctx, logFieldTreeWant, "tree Root", wantKey)
	foundKey, ok := o._want(ctx, wantKey)
	if !ok {
		o.enqueueRetry(btrfsprim.ROOT_TREE_OBJECTID)
		return 0, btrfsitem.Root{}, btrfstree.ErrNoItem
	}
	item, _ := discardErr(o.rebuilt.RebuiltTree(ctx, wantKey.TreeID)).TreeLookup(ctx, foundKey)
	defer item.Body.Free()
	switch itemBody := item.Body.(type) {
	case *btrfsitem.Root:
		return btrfsprim.Generation(foundKey.Offset), *itemBody, nil
	case *btrfsitem.Error:
		graphCallbacks(o).FSErr(ctx, fmt.Errorf("error decoding item: %v: %w", foundKey, itemBody.Err))
		return 0, btrfsitem.Root{}, itemBody.Err
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

// LookupUUID implements btrfsutil.RebuiltForrestCallbacks.
func (o forrestCallbacks) LookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, err error) {
	wantKey := wantWithTree{
		TreeID: btrfsprim.UUID_TREE_OBJECTID,
		Key:    wantFromKey(btrfsitem.UUIDToKey(uuid)),
	}
	ctx = withWant(ctx, logFieldTreeWant, "resolve parent UUID", wantKey)
	if !o._wantOff(ctx, wantKey) {
		o.enqueueRetry(btrfsprim.UUID_TREE_OBJECTID)
		return 0, btrfstree.ErrNoItem
	}
	item, _ := discardErr(o.rebuilt.RebuiltTree(ctx, wantKey.TreeID)).TreeLookup(ctx, wantKey.Key.Key())
	defer item.Body.Free()
	switch itemBody := item.Body.(type) {
	case *btrfsitem.UUIDMap:
		return itemBody.ObjID, nil
	case *btrfsitem.Error:
		graphCallbacks(o).FSErr(ctx, fmt.Errorf("error decoding item: %v: %w", wantKey, itemBody.Err))
		return 0, itemBody.Err
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}
