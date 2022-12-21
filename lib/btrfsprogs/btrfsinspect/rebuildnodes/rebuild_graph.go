// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	"reflect"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type rebuildCallbacks interface {
	fsErr(ctx context.Context, e error)
	want(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType)
	wantOff(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, off uint64)
	wantFunc(ctx context.Context, treeID btrfsprim.ObjID, objID btrfsprim.ObjID, typ btrfsprim.ItemType, fn func(btrfsitem.Item) bool)
	wantCSum(ctx context.Context, beg, end btrfsvol.LogicalAddr) // interval is [beg, end)
	wantFileExt(ctx context.Context, treeID btrfsprim.ObjID, ino btrfsprim.ObjID, size int64)
}

func handleItem(o rebuildCallbacks, ctx context.Context, treeID btrfsprim.ObjID, item btrfstree.Item) {
	ctx = dlog.WithField(ctx, "tree", treeID)
	ctx = dlog.WithField(ctx, "key", item.Key)

	// Notionally, just express the relationships shown in
	// https://btrfs.wiki.kernel.org/index.php/File:References.png (from the page
	// https://btrfs.wiki.kernel.org/index.php/Data_Structures )
	switch body := item.Body.(type) {
	case btrfsitem.BlockGroup:
		o.want(dlog.WithField(ctx, "wants", "Chunk"),
			btrfsprim.CHUNK_TREE_OBJECTID,
			body.ChunkObjectID,
			btrfsitem.CHUNK_ITEM_KEY)
		o.wantOff(dlog.WithField(ctx, "wants", "FreeSpaceInfo"),
			btrfsprim.FREE_SPACE_TREE_OBJECTID,
			item.Key.ObjectID,
			btrfsitem.FREE_SPACE_INFO_KEY,
			item.Key.Offset)
	case btrfsitem.Chunk:
		o.want(dlog.WithField(ctx, "wants", "owning Root"),
			btrfsprim.ROOT_TREE_OBJECTID,
			body.Head.Owner,
			btrfsitem.ROOT_ITEM_KEY)
	case btrfsitem.Dev:
		// nothing
	case btrfsitem.DevExtent:
		o.wantOff(dlog.WithField(ctx, "wants", "Chunk"),
			body.ChunkTree,
			body.ChunkObjectID,
			btrfsitem.CHUNK_ITEM_KEY,
			uint64(body.ChunkOffset))
	case btrfsitem.DevStats:
		// nothing
	case btrfsitem.DirEntry:
		// containing-directory
		o.wantOff(dlog.WithField(ctx, "wants", "containing dir inode"),
			treeID,
			item.Key.ObjectID,
			btrfsitem.INODE_ITEM_KEY,
			0)
		// siblings
		switch item.Key.ItemType {
		case btrfsitem.DIR_ITEM_KEY:
			o.wantFunc(dlog.WithField(ctx, "wants", "corresponding DIR_INDEX"),
				treeID,
				item.Key.ObjectID,
				btrfsitem.DIR_INDEX_KEY,
				func(peerItem btrfsitem.Item) bool {
					return reflect.DeepEqual(item, peerItem)
				})
		case btrfsitem.DIR_INDEX_KEY:
			o.wantOff(dlog.WithField(ctx, "wants", "corresponding DIR_ITEM"),
				treeID,
				item.Key.ObjectID,
				btrfsitem.DIR_ITEM_KEY,
				btrfsitem.NameHash(body.Name))
		case btrfsitem.XATTR_ITEM_KEY:
			// nothing
		default:
			// This is a panic because the item decoder should not emit a
			// btrfsitem.DirEntry for other item types without this code also being
			// updated.
			panic(fmt.Errorf("should not happen: DirEntry: unexpected ItemType=%v", item.Key.ItemType))
		}
		// item-within-directory
		if body.Location != (btrfsprim.Key{}) {
			switch body.Location.ItemType {
			case btrfsitem.INODE_ITEM_KEY:
				o.wantOff(dlog.WithField(ctx, "wants", "item being pointed to"),
					treeID,
					body.Location.ObjectID,
					body.Location.ItemType,
					body.Location.Offset)
				o.wantOff(dlog.WithField(ctx, "wants", "backref from item being pointed to"),
					treeID,
					body.Location.ObjectID,
					btrfsitem.INODE_REF_KEY,
					uint64(item.Key.ObjectID))
			case btrfsitem.ROOT_ITEM_KEY:
				o.want(dlog.WithField(ctx, "wants", "Root of subvolume being pointed to"),
					btrfsprim.ROOT_TREE_OBJECTID,
					body.Location.ObjectID,
					body.Location.ItemType)
			default:
				o.fsErr(ctx, fmt.Errorf("DirEntry: unexpected .Location.ItemType=%v", body.Location.ItemType))
			}
		}
	case btrfsitem.Empty:
		// nothing
	case btrfsitem.Extent:
		//if body.Head.Flags.Has(btrfsitem.EXTENT_FLAG_TREE_BLOCK) {
		//	// Supposedly this flag indicates that that
		//	// body.Info.Key identifies a node by the
		//	// first key in the node.  But nothing in the
		//	// kernel ever reads this, so who knows if it
		//	// always gets updated correctly?
		//}
		for i, ref := range body.Refs {
			switch refBody := ref.Body.(type) {
			case nil:
				// nothing
			case btrfsitem.ExtentDataRef:
				o.wantOff(dlog.WithField(ctx, "wants", "referencing Inode"),
					refBody.Root,
					refBody.ObjectID,
					btrfsitem.INODE_ITEM_KEY,
					0)
				o.wantOff(dlog.WithField(ctx, "wants", "referencing FileExtent"),
					refBody.Root,
					refBody.ObjectID,
					btrfsitem.EXTENT_DATA_KEY,
					uint64(refBody.Offset))
			case btrfsitem.SharedDataRef:
				// nothing
			default:
				// This is a panic because the item decoder should not emit a new
				// type to ref.Body without this code also being updated.
				panic(fmt.Errorf("should not happen: Extent: unexpected .Refs[%d].Body type %T", i, refBody))
			}
		}
	case btrfsitem.ExtentCSum:
		// nothing
	case btrfsitem.ExtentDataRef:
		o.want(dlog.WithField(ctx, "wants", "Extent being referenced"),
			btrfsprim.EXTENT_TREE_OBJECTID,
			item.Key.ObjectID,
			btrfsitem.EXTENT_ITEM_KEY)
		o.wantOff(dlog.WithField(ctx, "wants", "referencing Inode"),
			body.Root,
			body.ObjectID,
			btrfsitem.INODE_ITEM_KEY,
			0)
		o.wantOff(dlog.WithField(ctx, "wants", "referencing FileExtent"),
			body.Root,
			body.ObjectID,
			btrfsitem.EXTENT_DATA_KEY,
			uint64(body.Offset))
	case btrfsitem.FileExtent:
		o.wantOff(dlog.WithField(ctx, "wants", "containing Inode"),
			treeID,
			item.Key.ObjectID,
			btrfsitem.INODE_ITEM_KEY,
			0)
		switch body.Type {
		case btrfsitem.FILE_EXTENT_INLINE:
			// nothing
		case btrfsitem.FILE_EXTENT_REG, btrfsitem.FILE_EXTENT_PREALLOC:
			// TODO: Check if inodeBody.Flags.Has(btrfsitem.INODE_NODATASUM)
			o.wantCSum(dlog.WithField(ctx, "wants", "data sum"),
				roundDown(body.BodyExtent.DiskByteNr, btrfssum.BlockSize),
				roundUp(body.BodyExtent.DiskByteNr.Add(body.BodyExtent.DiskNumBytes), btrfssum.BlockSize))
		default:
			o.fsErr(ctx, fmt.Errorf("FileExtent: unexpected body.Type=%v", body.Type))
		}
	case btrfsitem.FreeSpaceBitmap:
		o.wantOff(dlog.WithField(ctx, "wants", "FreeSpaceInfo"),
			treeID,
			item.Key.ObjectID,
			btrfsitem.FREE_SPACE_INFO_KEY,
			item.Key.Offset)
	case btrfsitem.FreeSpaceHeader:
		o.wantOff(dlog.WithField(ctx, "wants", ".Location"),
			treeID,
			body.Location.ObjectID,
			body.Location.ItemType,
			body.Location.Offset)
	case btrfsitem.FreeSpaceInfo:
		if body.Flags.Has(btrfsitem.FREE_SPACE_USING_BITMAPS) {
			o.wantOff(dlog.WithField(ctx, "wants", "FreeSpaceBitmap"),
				treeID,
				item.Key.ObjectID,
				btrfsitem.FREE_SPACE_BITMAP_KEY,
				item.Key.Offset)
		}
	case btrfsitem.Inode:
		o.want(dlog.WithField(ctx, "wants", "backrefs"),
			treeID, // TODO: validate the number of these against body.NLink
			item.Key.ObjectID,
			btrfsitem.INODE_REF_KEY)
		o.wantFileExt(dlog.WithField(ctx, "wants", "FileExtents"),
			treeID, item.Key.ObjectID, body.Size)
		if body.BlockGroup != 0 {
			o.want(dlog.WithField(ctx, "wants", "BlockGroup"),
				btrfsprim.EXTENT_TREE_OBJECTID,
				body.BlockGroup,
				btrfsitem.BLOCK_GROUP_ITEM_KEY)
		}
	case btrfsitem.InodeRefs:
		o.wantOff(dlog.WithField(ctx, "wants", "child Inode"),
			treeID,
			item.Key.ObjectID,
			btrfsitem.INODE_ITEM_KEY,
			0)
		o.wantOff(dlog.WithField(ctx, "wants", "parent Inode"),
			treeID,
			btrfsprim.ObjID(item.Key.Offset),
			btrfsitem.INODE_ITEM_KEY,
			0)
		for _, ref := range body {
			o.wantOff(dlog.WithField(ctx, "wants", "DIR_ITEM"),
				treeID,
				btrfsprim.ObjID(item.Key.Offset),
				btrfsitem.DIR_ITEM_KEY,
				btrfsitem.NameHash(ref.Name))
			o.wantOff(dlog.WithField(ctx, "wants", "DIR_INDEX"),
				treeID,
				btrfsprim.ObjID(item.Key.Offset),
				btrfsitem.DIR_INDEX_KEY,
				uint64(ref.Index))
		}
	case btrfsitem.Metadata:
		for i, ref := range body.Refs {
			switch refBody := ref.Body.(type) {
			case nil:
				// nothing
			case btrfsitem.ExtentDataRef:
				o.wantOff(dlog.WithField(ctx, "wants", "referencing INode"),
					refBody.Root,
					refBody.ObjectID,
					btrfsitem.INODE_ITEM_KEY,
					0)
				o.wantOff(dlog.WithField(ctx, "wants", "referencing FileExtent"),
					refBody.Root,
					refBody.ObjectID,
					btrfsitem.EXTENT_DATA_KEY,
					uint64(refBody.Offset))
			case btrfsitem.SharedDataRef:
				// nothing
			default:
				// This is a panic because the item decoder should not emit a new
				// type to ref.Body without this code also being updated.
				panic(fmt.Errorf("should not happen: Metadata: unexpected .Refs[%d].Body type %T", i, refBody))
			}
		}
	case btrfsitem.Root:
		if body.RootDirID != 0 {
			o.wantOff(dlog.WithField(ctx, "wants", "root directory"),
				item.Key.ObjectID,
				body.RootDirID,
				btrfsitem.INODE_ITEM_KEY,
				0)
		}
		if body.UUID != (btrfsprim.UUID{}) {
			key := btrfsitem.UUIDToKey(body.UUID)
			o.wantOff(dlog.WithField(ctx, "wants", "uuid"),
				btrfsprim.UUID_TREE_OBJECTID,
				key.ObjectID,
				key.ItemType,
				key.Offset)
		}
		if body.ParentUUID != (btrfsprim.UUID{}) {
			key := btrfsitem.UUIDToKey(body.ParentUUID)
			o.wantOff(dlog.WithField(ctx, "wants", "parent uuid"),
				btrfsprim.UUID_TREE_OBJECTID,
				key.ObjectID,
				key.ItemType,
				key.Offset)
		}
	case btrfsitem.RootRef:
		var otherType btrfsprim.ItemType
		var parent, child btrfsprim.ObjID
		switch item.Key.ItemType {
		case btrfsitem.ROOT_REF_KEY:
			otherType = btrfsitem.ROOT_BACKREF_KEY
			parent = item.Key.ObjectID
			child = btrfsprim.ObjID(item.Key.Offset)
		case btrfsitem.ROOT_BACKREF_KEY:
			otherType = btrfsitem.ROOT_REF_KEY
			parent = btrfsprim.ObjID(item.Key.Offset)
			child = item.Key.ObjectID
		default:
			// This is a panic because the item decoder should not emit a
			// btrfsitem.RootRef for other item types without this code also being
			// updated.
			panic(fmt.Errorf("should not happen: RootRef: unexpected ItemType=%v", item.Key.ItemType))
		}
		// sibling
		o.wantOff(dlog.WithField(ctx, "wants", fmt.Sprintf("corresponding %v", otherType)),
			treeID,
			btrfsprim.ObjID(item.Key.Offset),
			otherType,
			uint64(item.Key.ObjectID))
		// parent
		o.want(dlog.WithField(ctx, "wants", "parent subvolume: Root"),
			treeID,
			parent,
			btrfsitem.ROOT_ITEM_KEY)
		o.wantOff(dlog.WithField(ctx, "wants", "parent subvolume: Inode of parent dir"),
			parent,
			body.DirID,
			btrfsitem.INODE_ITEM_KEY,
			0)
		o.wantOff(dlog.WithField(ctx, "wants", "parent subvolume: DIR_ITEM in parent dir"),
			parent,
			body.DirID,
			btrfsitem.DIR_ITEM_KEY,
			btrfsitem.NameHash(body.Name))
		o.wantOff(dlog.WithField(ctx, "wants", "parent subvolume: DIR_INDEX in parent dir"),
			parent,
			body.DirID,
			btrfsitem.DIR_INDEX_KEY,
			uint64(body.Sequence))
		// child
		o.want(dlog.WithField(ctx, "wants", "child subvolume: Root"),
			treeID,
			child,
			btrfsitem.ROOT_ITEM_KEY)
	case btrfsitem.SharedDataRef:
		o.want(dlog.WithField(ctx, "wants", "Extent"),
			btrfsprim.EXTENT_TREE_OBJECTID,
			item.Key.ObjectID,
			btrfsitem.EXTENT_ITEM_KEY)
	case btrfsitem.UUIDMap:
		o.want(dlog.WithField(ctx, "wants", "subvolume Root"),
			btrfsprim.ROOT_TREE_OBJECTID,
			body.ObjID,
			btrfsitem.ROOT_ITEM_KEY)
	case btrfsitem.Error:
		o.fsErr(ctx, fmt.Errorf("error decoding item: %w", body.Err))
	default:
		// This is a panic because the item decoder should not emit new types without this
		// code also being updated.
		panic(fmt.Errorf("should not happen: unexpected item type: %T", body))
	}
}
