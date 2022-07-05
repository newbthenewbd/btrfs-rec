package main

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/datawire/dlib/dcontext"
	"github.com/datawire/dlib/dlog"
	lru "github.com/hashicorp/golang-lru"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type Subvolume struct {
	FS         *btrfs.FS
	DeviceName string
	Mountpoint string
	TreeID     btrfs.ObjID

	rootOnce sync.Once
	rootVal  btrfsitem.Root
	rootErr  error

	inodeCache *lru.ARCCache
	dirCache   *lru.ARCCache

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

		sv.inodeCache, _ = lru.NewARC(128)
		sv.dirCache, _ = lru.NewARC(128)

		sv.subvolumeFUSE.init()
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

func (sv *Subvolume) loadInode(inode btrfs.ObjID) (btrfsitem.Inode, error) {
	tree, err := sv.getFSTree()
	if err != nil {
		return btrfsitem.Inode{}, nil
	}
	if ret, ok := sv.inodeCache.Get(inode); ok {
		return ret.(btrfsitem.Inode), nil
	}
	item, err := sv.FS.TreeLookup(tree, btrfs.Key{
		ObjectID: inode,
		ItemType: btrfsitem.INODE_ITEM_KEY,
		Offset:   0,
	})
	if err != nil {
		return btrfsitem.Inode{}, err
	}

	itemBody, ok := item.Body.(btrfsitem.Inode)
	if !ok {
		return btrfsitem.Inode{}, fmt.Errorf("malformed inode")
	}

	sv.inodeCache.Add(inode, itemBody)
	return itemBody, nil
}

type dir struct {
	Inode           btrfs.ObjID
	InodeDat        *btrfsitem.Inode
	ChildrenByName  map[string]btrfsitem.DirEntry
	ChildrenByIndex map[uint64]btrfsitem.DirEntry
	Errs            []error
}

func (sv *Subvolume) loadDir(inode btrfs.ObjID) (*dir, error) {
	tree, err := sv.getFSTree()
	if err != nil {
		return nil, err
	}
	if ret, ok := sv.dirCache.Get(inode); ok {
		return ret.(*dir), nil
	}
	ret := &dir{
		Inode:           inode,
		ChildrenByName:  make(map[string]btrfsitem.DirEntry),
		ChildrenByIndex: make(map[uint64]btrfsitem.DirEntry),
	}
	items, err := sv.FS.TreeSearchAll(tree, func(key btrfs.Key) int {
		return util.CmpUint(inode, key.ObjectID)
	})
	if err != nil {
		if len(items) == 0 {
			return nil, err
		}
		ret.Errs = append(ret.Errs, err)
	}
	for _, item := range items {
		switch item.Head.Key.ItemType {
		case btrfsitem.INODE_ITEM_KEY:
			itemBody := item.Body.(btrfsitem.Inode)
			if ret.InodeDat != nil {
				if !reflect.DeepEqual(itemBody, *ret.InodeDat) {
					ret.Errs = append(ret.Errs, fmt.Errorf("multiple inodes"))
				}
				continue
			}
			ret.InodeDat = &itemBody
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
			ret.Errs = append(ret.Errs, fmt.Errorf("TODO: handle item type %v", item.Head.Key.ItemType))
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
	sv.dirCache.Add(inode, ret)
	return ret, nil
}
