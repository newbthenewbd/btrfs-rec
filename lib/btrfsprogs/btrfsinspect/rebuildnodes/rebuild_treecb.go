// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"

	"github.com/datawire/dlib/dlog"

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
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.reason", "tree Root")
	key := keyAndTree{
		TreeID: btrfsprim.ROOT_TREE_OBJECTID,
		Key: btrfsprim.Key{
			ObjectID: tree,
			ItemType: btrfsitem.ROOT_ITEM_KEY,
		},
	}
	wantKey := fmt.Sprintf("tree=%v key={%v %v ?}", key.TreeID, key.ObjectID, key.ItemType)
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.key", wantKey)
	key.Key, ok = o._want(ctx, key.TreeID, wantKey, key.ObjectID, key.ItemType)
	if !ok {
		o.enqueueRetry()
		return 0, btrfsitem.Root{}, false
	}
	switch itemBody := o.rebuilt.Tree(ctx, key.TreeID).ReadItem(ctx, key.Key).(type) {
	case *btrfsitem.Root:
		return btrfsprim.Generation(key.Offset), *itemBody, true
	case *btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: %v: %w", key, itemBody.Err))
		return 0, btrfsitem.Root{}, false
	default:
		// This is a panic because the item decoder should not emit ROOT_ITEM items as anything but
		// btrfsitem.Root or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: ROOT_ITEM item has unexpected type: %T", itemBody))
	}
}

func (o *rebuilder) cbLookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.reason", "resolve parent UUID")
	key := keyAndTree{TreeID: btrfsprim.UUID_TREE_OBJECTID, Key: btrfsitem.UUIDToKey(uuid)}
	ctx = dlog.WithField(ctx, "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.key", key.String())
	if !o._wantOff(ctx, key.TreeID, key.String(), key.Key) {
		o.enqueueRetry()
		return 0, false
	}
	switch itemBody := o.rebuilt.Tree(ctx, key.TreeID).ReadItem(ctx, key.Key).(type) {
	case *btrfsitem.UUIDMap:
		return itemBody.ObjID, true
	case *btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: %v: %w", key, itemBody.Err))
		return 0, false
	default:
		// This is a panic because the item decoder should not emit UUID_SUBVOL items as anything but
		// btrfsitem.UUIDMap or btrfsitem.Error without this code also being updated.
		panic(fmt.Errorf("should not happen: UUID_SUBVOL item has unexpected type: %T", itemBody))
	}
}
