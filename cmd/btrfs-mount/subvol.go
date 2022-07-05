package main

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/datawire/dlib/dcontext"
	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type bareInode struct {
	Inode     btrfs.ObjID
	InodeItem *btrfsitem.Inode
	Errs      derror.MultiError
}

type fullInode struct {
	bareInode
	OtherItems []btrfs.Item
}

type dir struct {
	fullInode
	ChildrenByName  map[string]btrfsitem.DirEntry
	ChildrenByIndex map[uint64]btrfsitem.DirEntry
}

type file struct {
	fullInode
	// TODO
}

type Subvolume struct {
	FS         *btrfs.FS
	DeviceName string
	Mountpoint string
	TreeID     btrfs.ObjID

	rootOnce sync.Once
	rootVal  btrfsitem.Root
	rootErr  error

	bareInodeCache LRUCache[btrfs.ObjID, *bareInode]
	fullInodeCache LRUCache[btrfs.ObjID, *fullInode]
	dirCache       LRUCache[btrfs.ObjID, *dir]
	fileCache      LRUCache[btrfs.ObjID, *file]

	subvolumeFUSE
}

func (sv *Subvolume) Run(ctx context.Context) error {
	mount, err := fuse.Mount(
		sv.Mountpoint,
		fuseutil.NewFileSystemServer(sv),
		&fuse.MountConfig{
			OpContext:   ctx,
			ErrorLogger: dlog.StdLogger(ctx, dlog.LogLevelError),
			DebugLogger: dlog.StdLogger(ctx, dlog.LogLevelDebug),

			FSName:  sv.DeviceName,
			Subtype: "btrfs",

			ReadOnly: true,
		})
	if err != nil {
		return err
	}
	return mount.Join(dcontext.HardContext(ctx))
}

func (sv *Subvolume) init() {
	sv.rootOnce.Do(func() {
		sb, err := sv.FS.Superblock()
		if err != nil {
			sv.rootErr = err
			return
		}

		root, err := sv.FS.TreeLookup(sb.Data.RootTree, btrfs.Key{
			ObjectID: sv.TreeID,
			ItemType: btrfsitem.ROOT_ITEM_KEY,
			Offset:   0,
		})
		if err != nil {
			sv.rootErr = err
			return
		}

		rootBody, ok := root.Body.(btrfsitem.Root)
		if !ok {
			sv.rootErr = fmt.Errorf("FS_TREE_ ROOT_ITEM has malformed body")
			return
		}

		sv.rootVal = rootBody
	})
}

func (sv *Subvolume) getRootInode() (btrfs.ObjID, error) {
	sv.init()
	return sv.rootVal.RootDirID, sv.rootErr
}

func (sv *Subvolume) getFSTree() (btrfsvol.LogicalAddr, error) {
	sv.init()
	return sv.rootVal.ByteNr, sv.rootErr
}

func (sv *Subvolume) loadBareInode(inode btrfs.ObjID) (*bareInode, error) {
	val := sv.bareInodeCache.GetOrElse(inode, func() (val *bareInode) {
		val = &bareInode{
			Inode: inode,
		}
		tree, err := sv.getFSTree()
		if err != nil {
			val.Errs = append(val.Errs, err)
			return
		}
		item, err := sv.FS.TreeLookup(tree, btrfs.Key{
			ObjectID: inode,
			ItemType: btrfsitem.INODE_ITEM_KEY,
			Offset:   0,
		})
		if err != nil {
			val.Errs = append(val.Errs, err)
			return
		}

		itemBody, ok := item.Body.(btrfsitem.Inode)
		if !ok {
			val.Errs = append(val.Errs, fmt.Errorf("malformed inode"))
			return
		}
		val.InodeItem = &itemBody

		return
	})
	if val.InodeItem == nil {
		return nil, val.Errs
	}
	return val, nil
}

func (sv *Subvolume) loadFullInode(inode btrfs.ObjID) (*fullInode, error) {
	val := sv.fullInodeCache.GetOrElse(inode, func() (val *fullInode) {
		val = &fullInode{
			bareInode: bareInode{
				Inode: inode,
			},
		}
		tree, err := sv.getFSTree()
		if err != nil {
			val.Errs = append(val.Errs, err)
			return
		}
		items, err := sv.FS.TreeSearchAll(tree, func(key btrfs.Key) int {
			return util.CmpUint(inode, key.ObjectID)
		})
		if err != nil {
			val.Errs = append(val.Errs, err)
			if len(items) == 0 {
				return
			}
		}
		for _, item := range items {
			switch item.Head.Key.ItemType {
			case btrfsitem.INODE_ITEM_KEY:
				itemBody := item.Body.(btrfsitem.Inode)
				if val.InodeItem != nil {
					if !reflect.DeepEqual(itemBody, *val.InodeItem) {
						val.Errs = append(val.Errs, fmt.Errorf("multiple inodes"))
					}
					continue
				}
				val.InodeItem = &itemBody
			default:
				val.OtherItems = append(val.OtherItems, item)
			}
		}
		return
	})
	if val.InodeItem == nil && val.OtherItems == nil {
		return nil, val.Errs
	}
	return val, nil
}

func (sv *Subvolume) loadDir(inode btrfs.ObjID) (*dir, error) {
	val := sv.dirCache.GetOrElse(inode, func() (val *dir) {
		val = new(dir)
		fullInode, err := sv.loadFullInode(inode)
		if err != nil {
			val.Errs = append(val.Errs, err)
			return
		}
		val.fullInode = *fullInode
		val.populate()
		return
	})
	if val.Inode == 0 {
		return nil, val.Errs
	}
	return val, nil
}

func (ret *dir) populate() {
	ret.ChildrenByName = make(map[string]btrfsitem.DirEntry)
	ret.ChildrenByIndex = make(map[uint64]btrfsitem.DirEntry)
	for _, item := range ret.OtherItems {
		switch item.Head.Key.ItemType {
		case btrfsitem.INODE_REF_KEY:
			// TODO
		case btrfsitem.DIR_ITEM_KEY:
			body := item.Body.(btrfsitem.DirEntries)
			if len(body) != 1 {
				ret.Errs = append(ret.Errs, fmt.Errorf("multiple direntries in single DIR_ITEM?"))
				continue
			}
			for _, entry := range body {
				namehash := btrfsitem.NameHash(entry.Name)
				if namehash != item.Head.Key.Offset {
					ret.Errs = append(ret.Errs, fmt.Errorf("direntry crc32c mismatch: key=%#x crc32c(%q)=%#x",
						item.Head.Key.Offset, entry.Name, namehash))
					continue
				}
				if other, exists := ret.ChildrenByName[string(entry.Name)]; exists {
					if !reflect.DeepEqual(entry, other) {
						ret.Errs = append(ret.Errs, fmt.Errorf("multiple instances of direntry name %q", entry.Name))
					}
					continue
				}
				ret.ChildrenByName[string(entry.Name)] = entry
			}
		case btrfsitem.DIR_INDEX_KEY:
			index := item.Head.Key.Offset
			body := item.Body.(btrfsitem.DirEntries)
			if len(body) != 1 {
				ret.Errs = append(ret.Errs, fmt.Errorf("multiple direntries in single DIR_INDEX?"))
				continue
			}
			for _, entry := range body {
				if other, exists := ret.ChildrenByIndex[index]; exists {
					if !reflect.DeepEqual(entry, other) {
						ret.Errs = append(ret.Errs, fmt.Errorf("multiple instances of direntry index %v", index))
					}
					continue
				}
				ret.ChildrenByIndex[index] = entry
			}
		//case btrfsitem.XATTR_ITEM_KEY:
		default:
			panic(fmt.Errorf("TODO: handle item type %v", item.Head.Key.ItemType))
		}
	}
	entriesWithIndexes := make(map[string]struct{})
	nextIndex := uint64(2)
	for index, entry := range ret.ChildrenByIndex {
		if index+1 > nextIndex {
			nextIndex = index + 1
		}
		entriesWithIndexes[string(entry.Name)] = struct{}{}
		if other, exists := ret.ChildrenByName[string(entry.Name)]; !exists {
			ret.Errs = append(ret.Errs, fmt.Errorf("missing by-name direntry for %q", entry.Name))
			ret.ChildrenByName[string(entry.Name)] = entry
		} else if !reflect.DeepEqual(entry, other) {
			ret.Errs = append(ret.Errs, fmt.Errorf("direntry index %v and direntry name %q disagree", index, entry.Name))
			ret.ChildrenByName[string(entry.Name)] = entry
		}
	}
	for _, name := range util.SortedMapKeys(ret.ChildrenByName) {
		if _, exists := entriesWithIndexes[name]; !exists {
			ret.Errs = append(ret.Errs, fmt.Errorf("missing by-index direntry for %q", name))
			ret.ChildrenByIndex[nextIndex] = ret.ChildrenByName[name]
			nextIndex++
		}
	}
	return
}

func (sv *Subvolume) loadFile(inode btrfs.ObjID) (*file, error) {
	val := sv.fileCache.GetOrElse(inode, func() (val *file) {
		val = new(file)
		fullInode, err := sv.loadFullInode(inode)
		if err != nil {
			val.Errs = append(val.Errs, err)
			return
		}
		val.fullInode = *fullInode
		// TODO
		return
	})
	if val.Inode == 0 {
		return nil, val.Errs
	}
	return val, nil
}
