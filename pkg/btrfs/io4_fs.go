package btrfs

import (
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"
	"sync"

	"github.com/datawire/dlib/derror"

	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type BareInode struct {
	Inode     ObjID
	InodeItem *btrfsitem.Inode
	Errs      derror.MultiError
}

type FullInode struct {
	BareInode
	OtherItems []Item
}

type InodeRef struct {
	Inode ObjID
	btrfsitem.InodeRef
}

type Dir struct {
	FullInode
	DotDot          *InodeRef
	ChildrenByName  map[string]btrfsitem.DirEntry
	ChildrenByIndex map[uint64]btrfsitem.DirEntry
	SV              *Subvolume
}

type FileExtent struct {
	OffsetWithinFile int64
	btrfsitem.FileExtent
}

type File struct {
	FullInode
	Extents []FileExtent
	SV      *Subvolume
}

type Subvolume struct {
	FS     *FS
	TreeID ObjID

	rootOnce sync.Once
	rootVal  btrfsitem.Root
	rootErr  error

	bareInodeCache util.LRUCache[ObjID, *BareInode]
	fullInodeCache util.LRUCache[ObjID, *FullInode]
	dirCache       util.LRUCache[ObjID, *Dir]
	fileCache      util.LRUCache[ObjID, *File]
}

func (sv *Subvolume) init() {
	sv.rootOnce.Do(func() {
		root, err := sv.FS.TreeLookup(ROOT_TREE_OBJECTID, Key{
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
			sv.rootErr = fmt.Errorf("FS_TREE ROOT_ITEM has malformed body")
			return
		}

		sv.rootVal = rootBody
	})
}

func (sv *Subvolume) GetRootInode() (ObjID, error) {
	sv.init()
	return sv.rootVal.RootDirID, sv.rootErr
}

func (sv *Subvolume) LoadBareInode(inode ObjID) (*BareInode, error) {
	val := sv.bareInodeCache.GetOrElse(inode, func() (val *BareInode) {
		val = &BareInode{
			Inode: inode,
		}
		item, err := sv.FS.TreeLookup(sv.TreeID, Key{
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

func (sv *Subvolume) LoadFullInode(inode ObjID) (*FullInode, error) {
	val := sv.fullInodeCache.GetOrElse(inode, func() (val *FullInode) {
		val = &FullInode{
			BareInode: BareInode{
				Inode: inode,
			},
		}
		items, err := sv.FS.TreeSearchAll(sv.TreeID, func(key Key) int {
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

func (sv *Subvolume) LoadDir(inode ObjID) (*Dir, error) {
	val := sv.dirCache.GetOrElse(inode, func() (val *Dir) {
		val = new(Dir)
		fullInode, err := sv.LoadFullInode(inode)
		if err != nil {
			val.Errs = append(val.Errs, err)
			return
		}
		val.FullInode = *fullInode
		val.SV = sv
		val.populate()
		return
	})
	if val.Inode == 0 {
		return nil, val.Errs
	}
	return val, nil
}

func (ret *Dir) populate() {
	ret.ChildrenByName = make(map[string]btrfsitem.DirEntry)
	ret.ChildrenByIndex = make(map[uint64]btrfsitem.DirEntry)
	for _, item := range ret.OtherItems {
		switch item.Head.Key.ItemType {
		case btrfsitem.INODE_REF_KEY:
			ref := InodeRef{
				Inode:    ObjID(item.Head.Key.Offset),
				InodeRef: item.Body.(btrfsitem.InodeRef),
			}
			if ret.DotDot != nil {
				if !reflect.DeepEqual(ref, *ret.DotDot) {
					ret.Errs = append(ret.Errs, fmt.Errorf("multiple INODE_REF items on a directory"))
				}
				continue
			}
			ret.DotDot = &ref
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
}

func (dir *Dir) AbsPath() (string, error) {
	rootInode, err := dir.SV.GetRootInode()
	if err != nil {
		return "", err
	}
	if rootInode == dir.Inode {
		return "/", nil
	}
	if dir.DotDot == nil {
		return "", fmt.Errorf("missing .. entry in dir inode %v", dir.Inode)
	}
	parent, err := dir.SV.LoadDir(dir.DotDot.Inode)
	if err != nil {
		return "", err
	}
	parentName, err := parent.AbsPath()
	if err != nil {
		return "", err
	}
	return filepath.Join(parentName, string(dir.DotDot.Name)), nil
}

func (sv *Subvolume) LoadFile(inode ObjID) (*File, error) {
	val := sv.fileCache.GetOrElse(inode, func() (val *File) {
		val = new(File)
		fullInode, err := sv.LoadFullInode(inode)
		if err != nil {
			val.Errs = append(val.Errs, err)
			return
		}
		val.FullInode = *fullInode
		val.SV = sv
		val.populate()
		return
	})
	if val.Inode == 0 {
		return nil, val.Errs
	}
	return val, nil
}

func (ret *File) populate() {
	for _, item := range ret.OtherItems {
		switch item.Head.Key.ItemType {
		case btrfsitem.INODE_REF_KEY:
			// TODO
		case btrfsitem.EXTENT_DATA_KEY:
			ret.Extents = append(ret.Extents, FileExtent{
				OffsetWithinFile: int64(item.Head.Key.Offset),
				FileExtent:       item.Body.(btrfsitem.FileExtent),
			})
		default:
			panic(fmt.Errorf("TODO: handle item type %v", item.Head.Key.ItemType))
		}
	}

	// These should already be sorted, because of the nature of
	// the btree; but this is a recovery tool for corrupt
	// filesystems, so go ahead and ensure that it's sorted.
	sort.Slice(ret.Extents, func(i, j int) bool {
		return ret.Extents[i].OffsetWithinFile < ret.Extents[j].OffsetWithinFile
	})

	pos := int64(0)
	for _, extent := range ret.Extents {
		if extent.OffsetWithinFile != pos {
			if extent.OffsetWithinFile > pos {
				ret.Errs = append(ret.Errs, fmt.Errorf("extent gap from %v to %v",
					pos, extent.OffsetWithinFile))
			} else {
				ret.Errs = append(ret.Errs, fmt.Errorf("extent overlap from %v to %v",
					extent.OffsetWithinFile, pos))
			}
		}
		size, err := extent.Size()
		if err != nil {
			ret.Errs = append(ret.Errs, fmt.Errorf("extent %v: %w", extent.OffsetWithinFile, err))
		}
		pos += size
	}
	if ret.InodeItem != nil && pos != ret.InodeItem.Size {
		if ret.InodeItem.Size > pos {
			ret.Errs = append(ret.Errs, fmt.Errorf("extent gap from %v to %v",
				pos, ret.InodeItem.Size))
		} else {
			ret.Errs = append(ret.Errs, fmt.Errorf("extent mapped past end of file from %v to %v",
				ret.InodeItem.Size, pos))
		}
	}
}

func (file *File) ReadAt(dat []byte, off int64) (int, error) {
	// These stateles maybe-short-reads each do an O(n) extent
	// lookup, so reading a file is O(n^2), but we expect n to be
	// small, so whatev.  Turn file.Extents it in to an rbtree if
	// it becomes a problem.
	done := 0
	for done < len(dat) {
		n, err := file.maybeShortReadAt(dat[done:], off+int64(done))
		done += n
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

func (file *File) maybeShortReadAt(dat []byte, off int64) (int, error) {
	for _, extent := range file.Extents {
		extBeg := extent.OffsetWithinFile
		if extBeg > off {
			break
		}
		extLen, err := extent.Size()
		if err != nil {
			continue
		}
		extEnd := extBeg + extLen
		if extEnd <= off {
			continue
		}
		offsetWithinExt := off - extent.OffsetWithinFile
		readSize := util.Min(int64(len(dat)), extLen-offsetWithinExt)
		switch extent.Type {
		case btrfsitem.FILE_EXTENT_INLINE:
			return copy(dat, extent.BodyInline[offsetWithinExt:offsetWithinExt+readSize]), nil
		case btrfsitem.FILE_EXTENT_REG, btrfsitem.FILE_EXTENT_PREALLOC:
			return file.SV.FS.ReadAt(dat[:readSize],
				extent.BodyExtent.DiskByteNr.
					Add(extent.BodyExtent.Offset).
					Add(btrfsvol.AddrDelta(offsetWithinExt)))
		}
	}
	if file.InodeItem != nil && off >= file.InodeItem.Size {
		return 0, io.EOF
	}
	return 0, fmt.Errorf("read: could not map position %v", off)
}

var _ io.ReaderAt = (*File)(nil)
