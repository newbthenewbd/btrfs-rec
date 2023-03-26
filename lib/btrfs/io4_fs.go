// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/datawire/dlib/derror"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/caching"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type BareInode struct {
	Inode     btrfsprim.ObjID
	InodeItem *btrfsitem.Inode
	Errs      derror.MultiError
}

type FullInode struct {
	BareInode
	XAttrs     map[string]string
	OtherItems []btrfstree.Item
}

type InodeRef struct {
	Inode btrfsprim.ObjID
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
	ctx context.Context //nolint:containedctx // don't have an option while keeping the same API
	fs  interface {
		btrfstree.TreeOperator
		Superblock() (*btrfstree.Superblock, error)
		diskio.ReaderAt[btrfsvol.LogicalAddr]
	}
	TreeID      btrfsprim.ObjID
	noChecksums bool

	rootInfo btrfstree.TreeRoot
	rootErr  error

	bareInodeCache caching.Cache[btrfsprim.ObjID, BareInode]
	fullInodeCache caching.Cache[btrfsprim.ObjID, FullInode]
	dirCache       caching.Cache[btrfsprim.ObjID, Dir]
	fileCache      caching.Cache[btrfsprim.ObjID, File]
}

func NewSubvolume(
	ctx context.Context,
	fs interface {
		btrfstree.TreeOperator
		Superblock() (*btrfstree.Superblock, error)
		diskio.ReaderAt[btrfsvol.LogicalAddr]
	},
	treeID btrfsprim.ObjID,
	noChecksums bool,
) *Subvolume {
	sv := &Subvolume{
		fs:          fs,
		TreeID:      treeID,
		noChecksums: noChecksums,
	}

	sb, err := sv.fs.Superblock()
	if err != nil {
		sv.rootErr = err
		return sv
	}
	rootInfo, err := btrfstree.LookupTreeRoot(ctx, sv.fs, *sb, sv.TreeID)
	if err != nil {
		sv.rootErr = err
		return sv
	}
	sv.rootInfo = *rootInfo

	sv.bareInodeCache = caching.NewLRUCache[btrfsprim.ObjID, BareInode](textui.Tunable(128),
		caching.FuncSource[btrfsprim.ObjID, BareInode](sv.loadBareInode))
	sv.fullInodeCache = caching.NewLRUCache[btrfsprim.ObjID, FullInode](textui.Tunable(128),
		caching.FuncSource[btrfsprim.ObjID, FullInode](sv.loadFullInode))
	sv.dirCache = caching.NewLRUCache[btrfsprim.ObjID, Dir](textui.Tunable(128),
		caching.FuncSource[btrfsprim.ObjID, Dir](sv.loadDir))
	sv.fileCache = caching.NewLRUCache[btrfsprim.ObjID, File](textui.Tunable(128),
		caching.FuncSource[btrfsprim.ObjID, File](sv.loadFile))

	return sv
}

func (sv *Subvolume) NewChildSubvolume(childID btrfsprim.ObjID) *Subvolume {
	return NewSubvolume(sv.ctx, sv.fs, childID, sv.noChecksums)
}

func (sv *Subvolume) GetRootInode() (btrfsprim.ObjID, error) {
	return sv.rootInfo.RootInode, sv.rootErr
}

func (sv *Subvolume) AcquireBareInode(inode btrfsprim.ObjID) (*BareInode, error) {
	val := sv.bareInodeCache.Acquire(sv.ctx, inode)
	if val.InodeItem == nil {
		sv.bareInodeCache.Release(inode)
		return nil, val.Errs
	}
	return val, nil
}

func (sv *Subvolume) ReleaseBareInode(inode btrfsprim.ObjID) {
	sv.bareInodeCache.Release(inode)
}

func (sv *Subvolume) loadBareInode(_ context.Context, inode btrfsprim.ObjID, val *BareInode) {
	*val = BareInode{
		Inode: inode,
	}
	item, err := sv.fs.TreeLookup(sv.TreeID, btrfsprim.Key{
		ObjectID: inode,
		ItemType: btrfsitem.INODE_ITEM_KEY,
		Offset:   0,
	})
	if err != nil {
		val.Errs = append(val.Errs, err)
		return
	}

	switch itemBody := item.Body.(type) {
	case *btrfsitem.Inode:
		bodyCopy := itemBody.Clone()
		val.InodeItem = &bodyCopy
	case *btrfsitem.Error:
		val.Errs = append(val.Errs, fmt.Errorf("malformed inode: %w", itemBody.Err))
	default:
		panic(fmt.Errorf("should not happen: INODE_ITEM has unexpected item type: %T", itemBody))
	}
}

func (sv *Subvolume) AcquireFullInode(inode btrfsprim.ObjID) (*FullInode, error) {
	val := sv.fullInodeCache.Acquire(sv.ctx, inode)
	if val.InodeItem == nil && val.OtherItems == nil {
		sv.fullInodeCache.Release(inode)
		return nil, val.Errs
	}
	return val, nil
}

func (sv *Subvolume) ReleaseFullInode(inode btrfsprim.ObjID) {
	sv.fullInodeCache.Release(inode)
}

func (sv *Subvolume) loadFullInode(_ context.Context, inode btrfsprim.ObjID, val *FullInode) {
	*val = FullInode{
		BareInode: BareInode{
			Inode: inode,
		},
		XAttrs: make(map[string]string),
	}
	items, err := sv.fs.TreeSearchAll(sv.TreeID, btrfstree.SearchObject(inode))
	if err != nil {
		val.Errs = append(val.Errs, err)
		if len(items) == 0 {
			return
		}
	}
	for _, item := range items {
		switch item.Key.ItemType {
		case btrfsitem.INODE_ITEM_KEY:
			switch itemBody := item.Body.(type) {
			case *btrfsitem.Inode:
				if val.InodeItem != nil {
					if !reflect.DeepEqual(itemBody, *val.InodeItem) {
						val.Errs = append(val.Errs, fmt.Errorf("multiple inodes"))
					}
					continue
				}
				bodyCopy := itemBody.Clone()
				val.InodeItem = &bodyCopy
			case *btrfsitem.Error:
				val.Errs = append(val.Errs, fmt.Errorf("malformed INODE_ITEM: %w", itemBody.Err))
			default:
				panic(fmt.Errorf("should not happen: INODE_ITEM has unexpected item type: %T", itemBody))
			}
		case btrfsitem.XATTR_ITEM_KEY:
			switch itemBody := item.Body.(type) {
			case *btrfsitem.DirEntry:
				val.XAttrs[string(itemBody.Name)] = string(itemBody.Data)
			case *btrfsitem.Error:
				val.Errs = append(val.Errs, fmt.Errorf("malformed XATTR_ITEM: %w", itemBody.Err))
			default:
				panic(fmt.Errorf("should not happen: XATTR_ITEM has unexpected item type: %T", itemBody))
			}
		default:
			val.OtherItems = append(val.OtherItems, item)
		}
	}
}

func (sv *Subvolume) AcquireDir(inode btrfsprim.ObjID) (*Dir, error) {
	val := sv.dirCache.Acquire(sv.ctx, inode)
	if val.Inode == 0 {
		sv.dirCache.Release(inode)
		return nil, val.Errs
	}
	return val, nil
}

func (sv *Subvolume) ReleaseDir(inode btrfsprim.ObjID) {
	sv.dirCache.Release(inode)
}

func (sv *Subvolume) loadDir(_ context.Context, inode btrfsprim.ObjID, dir *Dir) {
	*dir = Dir{}
	fullInode, err := sv.AcquireFullInode(inode)
	if err != nil {
		dir.Errs = append(dir.Errs, err)
		return
	}
	dir.FullInode = *fullInode
	sv.ReleaseFullInode(inode)
	dir.SV = sv

	dir.ChildrenByName = make(map[string]btrfsitem.DirEntry)
	dir.ChildrenByIndex = make(map[uint64]btrfsitem.DirEntry)
	for _, item := range dir.OtherItems {
		switch item.Key.ItemType {
		case btrfsitem.INODE_REF_KEY:
			switch body := item.Body.(type) {
			case *btrfsitem.InodeRefs:
				if len(body.Refs) != 1 {
					dir.Errs = append(dir.Errs, fmt.Errorf("INODE_REF item with %d entries on a directory",
						len(body.Refs)))
					continue
				}
				ref := InodeRef{
					Inode:    btrfsprim.ObjID(item.Key.Offset),
					InodeRef: body.Refs[0],
				}
				if dir.DotDot != nil {
					if !reflect.DeepEqual(ref, *dir.DotDot) {
						dir.Errs = append(dir.Errs, fmt.Errorf("multiple INODE_REF items on a directory"))
					}
					continue
				}
				dir.DotDot = &ref
			case *btrfsitem.Error:
				dir.Errs = append(dir.Errs, fmt.Errorf("malformed INODE_REF: %w", body.Err))
			default:
				panic(fmt.Errorf("should not happen: INODE_REF has unexpected item type: %T", body))
			}
		case btrfsitem.DIR_ITEM_KEY:
			switch entry := item.Body.(type) {
			case *btrfsitem.DirEntry:
				namehash := btrfsitem.NameHash(entry.Name)
				if namehash != item.Key.Offset {
					dir.Errs = append(dir.Errs, fmt.Errorf("direntry crc32c mismatch: key=%#x crc32c(%q)=%#x",
						item.Key.Offset, entry.Name, namehash))
					continue
				}
				if other, exists := dir.ChildrenByName[string(entry.Name)]; exists {
					if !reflect.DeepEqual(entry, other) {
						dir.Errs = append(dir.Errs, fmt.Errorf("multiple instances of direntry name %q", entry.Name))
					}
					continue
				}
				dir.ChildrenByName[string(entry.Name)] = entry.Clone()
			case *btrfsitem.Error:
				dir.Errs = append(dir.Errs, fmt.Errorf("malformed DIR_ITEM: %w", entry.Err))
			default:
				panic(fmt.Errorf("should not happen: DIR_ITEM has unexpected item type: %T", entry))
			}
		case btrfsitem.DIR_INDEX_KEY:
			index := item.Key.Offset
			switch entry := item.Body.(type) {
			case *btrfsitem.DirEntry:
				if other, exists := dir.ChildrenByIndex[index]; exists {
					if !reflect.DeepEqual(entry, other) {
						dir.Errs = append(dir.Errs, fmt.Errorf("multiple instances of direntry index %v", index))
					}
					continue
				}
				dir.ChildrenByIndex[index] = entry.Clone()
			case *btrfsitem.Error:
				dir.Errs = append(dir.Errs, fmt.Errorf("malformed DIR_INDEX: %w", entry.Err))
			default:
				panic(fmt.Errorf("should not happen: DIR_INDEX has unexpected item type: %T", entry))
			}
		default:
			panic(fmt.Errorf("TODO: handle item type %v", item.Key.ItemType))
		}
	}
	entriesWithIndexes := make(containers.Set[string])
	nextIndex := uint64(2)
	for _, index := range maps.SortedKeys(dir.ChildrenByIndex) {
		entry := dir.ChildrenByIndex[index]
		if index+1 > nextIndex {
			nextIndex = index + 1
		}
		entriesWithIndexes.Insert(string(entry.Name))
		if other, exists := dir.ChildrenByName[string(entry.Name)]; !exists {
			dir.Errs = append(dir.Errs, fmt.Errorf("missing by-name direntry for %q", entry.Name))
			dir.ChildrenByName[string(entry.Name)] = entry
		} else if !reflect.DeepEqual(entry, other) {
			dir.Errs = append(dir.Errs, fmt.Errorf("direntry index %v and direntry name %q disagree", index, entry.Name))
			dir.ChildrenByName[string(entry.Name)] = entry
		}
	}
	for _, name := range maps.SortedKeys(dir.ChildrenByName) {
		if !entriesWithIndexes.Has(name) {
			dir.Errs = append(dir.Errs, fmt.Errorf("missing by-index direntry for %q", name))
			dir.ChildrenByIndex[nextIndex] = dir.ChildrenByName[name]
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
	parent, err := dir.SV.AcquireDir(dir.DotDot.Inode)
	if err != nil {
		return "", err
	}
	parentName, err := parent.AbsPath()
	dir.SV.ReleaseDir(dir.DotDot.Inode)
	if err != nil {
		return "", err
	}
	return filepath.Join(parentName, string(dir.DotDot.Name)), nil
}

func (sv *Subvolume) AcquireFile(inode btrfsprim.ObjID) (*File, error) {
	val := sv.fileCache.Acquire(sv.ctx, inode)
	if val.Inode == 0 {
		sv.fileCache.Release(inode)
		return nil, val.Errs
	}
	return val, nil
}

func (sv *Subvolume) ReleaseFile(inode btrfsprim.ObjID) {
	sv.fileCache.Release(inode)
}

func (sv *Subvolume) loadFile(_ context.Context, inode btrfsprim.ObjID, file *File) {
	*file = File{}
	fullInode, err := sv.AcquireFullInode(inode)
	if err != nil {
		file.Errs = append(file.Errs, err)
		return
	}
	file.FullInode = *fullInode
	sv.ReleaseFullInode(inode)
	file.SV = sv

	for _, item := range file.OtherItems {
		switch item.Key.ItemType {
		case btrfsitem.INODE_REF_KEY:
			// TODO
		case btrfsitem.EXTENT_DATA_KEY:
			switch itemBody := item.Body.(type) {
			case *btrfsitem.FileExtent:
				file.Extents = append(file.Extents, FileExtent{
					OffsetWithinFile: int64(item.Key.Offset),
					FileExtent:       *itemBody,
				})
			case *btrfsitem.Error:
				file.Errs = append(file.Errs, fmt.Errorf("malformed EXTENT_DATA: %w", itemBody.Err))
			default:
				panic(fmt.Errorf("should not happen: EXTENT_DATA has unexpected item type: %T", itemBody))
			}
		default:
			panic(fmt.Errorf("TODO: handle item type %v", item.Key.ItemType))
		}
	}

	// These should already be sorted, because of the nature of
	// the btree; but this is a recovery tool for corrupt
	// filesystems, so go ahead and ensure that it's sorted.
	sort.Slice(file.Extents, func(i, j int) bool {
		return file.Extents[i].OffsetWithinFile < file.Extents[j].OffsetWithinFile
	})

	pos := int64(0)
	for _, extent := range file.Extents {
		if extent.OffsetWithinFile != pos {
			if extent.OffsetWithinFile > pos {
				file.Errs = append(file.Errs, fmt.Errorf("extent gap from %v to %v",
					pos, extent.OffsetWithinFile))
			} else {
				file.Errs = append(file.Errs, fmt.Errorf("extent overlap from %v to %v",
					extent.OffsetWithinFile, pos))
			}
		}
		size, err := extent.Size()
		if err != nil {
			file.Errs = append(file.Errs, fmt.Errorf("extent %v: %w", extent.OffsetWithinFile, err))
		}
		pos += size
	}
	if file.InodeItem != nil && pos != file.InodeItem.NumBytes {
		if file.InodeItem.NumBytes > pos {
			file.Errs = append(file.Errs, fmt.Errorf("extent gap from %v to %v",
				pos, file.InodeItem.NumBytes))
		} else {
			file.Errs = append(file.Errs, fmt.Errorf("extent mapped past end of file from %v to %v",
				file.InodeItem.NumBytes, pos))
		}
	}
}

func (file *File) ReadAt(dat []byte, off int64) (int, error) {
	// These stateless maybe-short-reads each do an O(n) extent
	// lookup, so reading a file is O(n^2), but we expect n to be
	// small, so whatev.  Turn file.Extents in to an rbtree if it
	// becomes a problem.
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
		readSize := slices.Min(int64(len(dat)), extLen-offsetWithinExt, btrfssum.BlockSize)
		switch extent.Type {
		case btrfsitem.FILE_EXTENT_INLINE:
			return copy(dat, extent.BodyInline[offsetWithinExt:offsetWithinExt+readSize]), nil
		case btrfsitem.FILE_EXTENT_REG, btrfsitem.FILE_EXTENT_PREALLOC:
			sb, err := file.SV.fs.Superblock()
			if err != nil {
				return 0, err
			}
			beg := extent.BodyExtent.DiskByteNr.
				Add(extent.BodyExtent.Offset).
				Add(btrfsvol.AddrDelta(offsetWithinExt))
			var block [btrfssum.BlockSize]byte
			blockBeg := (beg / btrfssum.BlockSize) * btrfssum.BlockSize
			n, err := file.SV.fs.ReadAt(block[:], blockBeg)
			if n > int(beg-blockBeg) {
				n = copy(dat[:readSize], block[beg-blockBeg:])
			} else {
				n = 0
			}
			if err != nil {
				return 0, err
			}
			if !file.SV.noChecksums {
				sumRun, err := LookupCSum(file.SV.fs, sb.ChecksumType, blockBeg)
				if err != nil {
					return 0, fmt.Errorf("checksum@%v: %w", blockBeg, err)
				}
				_expSum, ok := sumRun.SumForAddr(blockBeg)
				if !ok {
					panic(fmt.Errorf("run from LookupCSum(fs, typ, %v) did not contain %v: %#v",
						blockBeg, blockBeg, sumRun))
				}
				expSum := _expSum.ToFullSum()

				actSum, err := sb.ChecksumType.Sum(block[:])
				if err != nil {
					return 0, fmt.Errorf("checksum@%v: %w", blockBeg, err)
				}

				if actSum != expSum {
					return 0, fmt.Errorf("checksum@%v: actual sum %v != expected sum %v",
						blockBeg, actSum, expSum)
				}
			}
			return n, nil
		}
	}
	if file.InodeItem != nil && off >= file.InodeItem.Size {
		return 0, io.EOF
	}
	return 0, fmt.Errorf("read: could not map position %v", off)
}

var _ io.ReaderAt = (*File)(nil)
