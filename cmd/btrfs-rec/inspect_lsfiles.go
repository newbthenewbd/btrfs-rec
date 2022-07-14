// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:   "ls-files",
			Short: "A listing of all files in the filesystem",
			Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, _ *cobra.Command, _ []string) error {
			printSubvol(fs, "", "", "/", btrfs.Key{
				ObjectID: btrfs.FS_TREE_OBJECTID,
				ItemType: btrfsitem.ROOT_ITEM_KEY,
				Offset:   0,
			})
			return nil
		},
	})
}

const (
	tS = "    "
	tl = "│   "
	tT = "├── "
	tL = "└── "
)

func printSubvol(fs *btrfs.FS, prefix0, prefix1, name string, key btrfs.Key) {
	root, err := fs.TreeLookup(btrfs.ROOT_TREE_OBJECTID, key)
	if err != nil {
		fmt.Printf("%s%q error: could not look up root %v: %v\n", prefix0, name, key, err)
		return
	}
	rootBody := root.Body.(btrfsitem.Root)

	printDir(fs, root.Key.ObjectID, prefix0, prefix1, name, rootBody.RootDirID)
}

func printDir(fs *btrfs.FS, fsTree btrfs.ObjID, prefix0, prefix1, dirName string, dirInode btrfs.ObjID) {
	var errs derror.MultiError
	items, err := fs.TreeSearchAll(fsTree, func(key btrfs.Key) int {
		return containers.CmpUint(dirInode, key.ObjectID)
	})
	if err != nil {
		errs = append(errs, fmt.Errorf("read dir: %w", err))
	}
	var dirInodeDat btrfsitem.Inode
	var dirInodeDatOK bool
	membersByIndex := make(map[uint64]btrfsitem.DirEntry)
	membersByNameHash := make(map[uint64]btrfsitem.DirEntry)
	for _, item := range items {
		switch item.Key.ItemType {
		case btrfsitem.INODE_ITEM_KEY:
			if dirInodeDatOK {
				if !reflect.DeepEqual(dirInodeDat, item.Body.(btrfsitem.Inode)) {
					errs = append(errs, fmt.Errorf("read dir: multiple inodes"))
				}
				continue
			}
			dirInodeDat = item.Body.(btrfsitem.Inode)
			dirInodeDatOK = true
		case btrfsitem.INODE_REF_KEY:
			// TODO
		case btrfsitem.DIR_ITEM_KEY:
			entry := item.Body.(btrfsitem.DirEntry)
			namehash := btrfsitem.NameHash(entry.Name)
			if namehash != item.Key.Offset {
				errs = append(errs, fmt.Errorf("read dir: direntry crc32c mismatch: key=%#x crc32c(%q)=%#x",
					item.Key.Offset, entry.Name, namehash))
				continue
			}
			if other, exists := membersByNameHash[namehash]; exists {
				if !reflect.DeepEqual(entry, other) {
					if string(entry.Name) == string(other.Name) {
						errs = append(errs, fmt.Errorf("read dir: multiple instances of direntry crc32c(%q)=%#x",
							entry.Name, namehash))
					} else {
						errs = append(errs, fmt.Errorf("read dir: multiple instances of direntry crc32c(%q|%q)=%#x",
							other.Name, entry.Name, namehash))
					}
				}
				continue
			}
			membersByNameHash[btrfsitem.NameHash(entry.Name)] = entry
		case btrfsitem.DIR_INDEX_KEY:
			index := item.Key.Offset
			entry := item.Body.(btrfsitem.DirEntry)
			if other, exists := membersByIndex[index]; exists {
				if !reflect.DeepEqual(entry, other) {
					errs = append(errs, fmt.Errorf("read dir: multiple instances of direntry index %v", index))
				}
				continue
			}
			membersByIndex[index] = entry
		//case btrfsitem.XATTR_ITEM_KEY:
		default:
			errs = append(errs, fmt.Errorf("TODO: handle item type %v", item.Key.ItemType))
		}
	}
	fmt.Printf("%s%q\t[ino=%d",
		prefix0, dirName, dirInode)
	if dirInodeDatOK {
		fmt.Printf("\tuid=%d\tgid=%d\tsize=%d]\n",
			dirInodeDat.UID, dirInodeDat.GID, dirInodeDat.Size)
	} else {
		err := fmt.Errorf("read dir: no inode data")
		if len(items) == 0 && len(errs) == 1 {
			err = errs[0]
			errs = nil
		}
		fmt.Printf("]\terror: %v\n", err)
	}
	for i, index := range maps.SortedKeys(membersByIndex) {
		entry := membersByIndex[index]
		namehash := btrfsitem.NameHash(entry.Name)
		if other, ok := membersByNameHash[namehash]; ok {
			if !reflect.DeepEqual(entry, other) {
				errs = append(errs, fmt.Errorf("read dir: index=%d disagrees with crc32c(%q)=%#x",
					index, entry.Name, namehash))
			}
			delete(membersByNameHash, namehash)
		} else {
			errs = append(errs, fmt.Errorf("read dir: no DIR_ITEM crc32c(%q)=%#x for DIR_INDEX index=%d",
				entry.Name, namehash, index))
		}
		p0, p1 := tT, tl
		if (i == len(membersByIndex)-1) && (len(membersByNameHash) == 0) && (len(errs) == 0) {
			p0, p1 = tL, tS
		}
		printDirEntry(fs, fsTree, prefix1+p0, prefix1+p1, entry)
	}
	for _, namehash := range maps.SortedKeys(membersByNameHash) {
		entry := membersByNameHash[namehash]
		errs = append(errs, fmt.Errorf("read dir: no DIR_INDEX for DIR_ITEM crc32c(%q)=%#x",
			entry.Name, namehash))
		printDirEntry(fs, fsTree, prefix1+tT, prefix1+tl, entry)
	}
	for i, err := range errs {
		p0, p1 := tT, tl
		if i == len(errs)-1 {
			p0, p1 = tL, tS
		}
		fmt.Printf("%serror: %s\n", prefix1+p0, strings.ReplaceAll(err.Error(), "\n", prefix1+p1+"       \n"))
	}
}

func printDirEntry(fs *btrfs.FS, fsTree btrfs.ObjID, prefix0, prefix1 string, entry btrfsitem.DirEntry) {
	if len(entry.Data) != 0 {
		fmt.Printf("%s%q: error: TODO: I don't know how to handle dirent.data\n",
			prefix0, entry.Name)
		return
	}
	switch entry.Type {
	case btrfsitem.FT_DIR:
		switch entry.Location.ItemType {
		case btrfsitem.INODE_ITEM_KEY:
			printDir(fs, fsTree, prefix0, prefix1, string(entry.Name), entry.Location.ObjectID)
		case btrfsitem.ROOT_ITEM_KEY:
			key := entry.Location
			key.Offset = 0
			printSubvol(fs, prefix0, prefix1, string(entry.Name), key)
		default:
			fmt.Printf("%s%q\t[location=%v type=%v] error: I'm not sure how to print a %v directory\n",
				prefix0, entry.Name, entry.Location, entry.Type, entry.Location.ItemType)
		}
	default:
		fmt.Printf("%s%q\t[location=%v type=%v]\n", prefix0, entry.Name, entry.Location, entry.Type)
	}
}
