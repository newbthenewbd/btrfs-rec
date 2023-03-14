// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:   "ls-files",
			Short: "A listing of all files in the filesystem",
			Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) (err error) {
			out := bufio.NewWriter(os.Stdout)
			defer func() {
				if _err := out.Flush(); _err != nil && err == nil {
					err = _err
				}
			}()
			defer func() {
				if _err := derror.PanicToError(recover()); _err != nil {
					textui.Fprintf(out, "\n\n%+v\n", _err)
					err = _err
				}
			}()
			ctx := cmd.Context()

			printSubvol(out, "", true, "/", &btrfs.Subvolume{
				FS:     btrfsutil.NewBrokenTrees(ctx, fs),
				TreeID: btrfsprim.FS_TREE_OBJECTID,
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

func printText(out io.Writer, prefix string, isLast bool, name, text string) {
	first, rest := tT, tl
	if isLast {
		first, rest = tL, tS
	}
	for i, line := range strings.Split(textui.Sprintf("%q %s", name, text), "\n") {
		_, _ = io.WriteString(out, prefix)
		if i == 0 {
			_, _ = io.WriteString(out, first)
		} else {
			_, _ = io.WriteString(out, rest)
		}
		_, _ = io.WriteString(out, line)
		_, _ = io.WriteString(out, "\n")
	}
}

func printSubvol(out io.Writer, prefix string, isLast bool, name string, subvol *btrfs.Subvolume) {
	rootInode, err := subvol.GetRootInode()
	if err != nil {
		printText(out, prefix, isLast, name+"/", textui.Sprintf("subvol_id=%v err=%v",
			subvol.TreeID, fmtErr(err)))
		return
	}
	dir, err := subvol.LoadDir(rootInode)
	if err != nil {
		printText(out, prefix, isLast, name+"/", textui.Sprintf("subvol_id=%v err=%v",
			subvol.TreeID, fmtErr(err)))
		return
	}
	if name == "/" {
		printDir(out, prefix, isLast, name, dir)
		return
	}
	printText(out, prefix, isLast, name+"/", textui.Sprintf("subvol_id=%v", subvol.TreeID))
	if isLast {
		prefix += tS
	} else {
		prefix += tl
	}
	printDir(out, prefix, true, name, dir)
}

func fmtErr(err error) string {
	errStr := err.Error()
	if strings.Contains(errStr, "\n") {
		errStr = "\\\n" + errStr
	}
	return errStr
}

func fmtInode(inode btrfs.BareInode) string {
	var mode btrfsitem.StatMode
	if inode.InodeItem == nil {
		inode.Errs = append(inode.Errs, errors.New("missing INODE_ITEM"))
	} else {
		mode = inode.InodeItem.Mode
	}
	ret := textui.Sprintf("ino=%v mode=%v", inode.Inode, mode)
	if len(inode.Errs) > 0 {
		ret += " err=" + fmtErr(inode.Errs)
	}
	return ret
}

func printDir(out io.Writer, prefix string, isLast bool, name string, dir *btrfs.Dir) {
	printText(out, prefix, isLast, name+"/", fmtInode(dir.BareInode))
	if isLast {
		prefix += tS
	} else {
		prefix += tl
	}
	for i, childName := range maps.SortedKeys(dir.ChildrenByName) {
		printDirEntry(
			out,
			prefix,
			i == len(dir.ChildrenByName)-1,
			dir.SV,
			path.Join(name, childName),
			dir.ChildrenByName[childName])
	}
}

func printDirEntry(out io.Writer, prefix string, isLast bool, subvol *btrfs.Subvolume, name string, entry btrfsitem.DirEntry) {
	if len(entry.Data) != 0 {
		panic(fmt.Errorf("TODO: I don't know how to handle dirent.data: %q", name))
	}
	switch entry.Type {
	case btrfsitem.FT_DIR:
		switch entry.Location.ItemType {
		case btrfsitem.INODE_ITEM_KEY:
			dir, err := subvol.LoadDir(entry.Location.ObjectID)
			if err != nil {
				printText(out, prefix, isLast, name, textui.Sprintf("%v err=%v", entry.Type, fmtErr(err)))
				return
			}
			printDir(out, prefix, isLast, name, dir)
		case btrfsitem.ROOT_ITEM_KEY:
			printSubvol(out, prefix, isLast, name, &btrfs.Subvolume{
				FS:     subvol.FS,
				TreeID: entry.Location.ObjectID,
			})
		default:
			panic(fmt.Errorf("TODO: I don't know how to handle an FT_DIR with location.ItemType=%v: %q",
				entry.Location.ItemType, name))
		}
	case btrfsitem.FT_SYMLINK:
		if entry.Location.ItemType != btrfsitem.INODE_ITEM_KEY {
			panic(fmt.Errorf("TODO: I don't know how to handle an FT_SYMLINK with location.ItemType=%v: %q",
				entry.Location.ItemType, name))
		}
		file, err := subvol.LoadFile(entry.Location.ObjectID)
		if err != nil {
			printText(out, prefix, isLast, name, textui.Sprintf("%v err=%v", entry.Type, fmtErr(err)))
			return
		}
		printSymlink(out, prefix, isLast, name, file)
	case btrfsitem.FT_REG_FILE:
		if entry.Location.ItemType != btrfsitem.INODE_ITEM_KEY {
			panic(fmt.Errorf("TODO: I don't know how to handle an FT_REG_FILE with location.ItemType=%v: %q",
				entry.Location.ItemType, name))
		}
		file, err := subvol.LoadFile(entry.Location.ObjectID)
		if err != nil {
			printText(out, prefix, isLast, name, textui.Sprintf("%v err=%v", entry.Type, fmtErr(err)))
			return
		}
		printFile(out, prefix, isLast, name, file)
	case btrfsitem.FT_SOCK:
		if entry.Location.ItemType != btrfsitem.INODE_ITEM_KEY {
			panic(fmt.Errorf("TODO: I don't know how to handle an FT_SOCK with location.ItemType=%v: %q",
				entry.Location.ItemType, name))
		}
		file, err := subvol.LoadFile(entry.Location.ObjectID)
		if err != nil {
			printText(out, prefix, isLast, name, textui.Sprintf("%v err=%v", entry.Type, fmtErr(err)))
			return
		}
		printSocket(out, prefix, isLast, name, file)
	case btrfsitem.FT_FIFO:
		if entry.Location.ItemType != btrfsitem.INODE_ITEM_KEY {
			panic(fmt.Errorf("TODO: I don't know how to handle an FT_FIFO with location.ItemType=%v: %q",
				entry.Location.ItemType, name))
		}
		file, err := subvol.LoadFile(entry.Location.ObjectID)
		if err != nil {
			printText(out, prefix, isLast, name, textui.Sprintf("%v err=%v", entry.Type, fmtErr(err)))
			return
		}
		printPipe(out, prefix, isLast, name, file)
	default:
		panic(fmt.Errorf("TODO: I don't know how to handle an fileType=%v: %q",
			entry.Type, name))
	}
}

func printSymlink(out io.Writer, prefix string, isLast bool, name string, file *btrfs.File) {
	var tgt []byte
	if file.InodeItem != nil {
		var err error
		tgt, err = io.ReadAll(io.NewSectionReader(file, 0, file.InodeItem.Size))
		if err != nil {
			file.Errs = append(file.Errs, err)
		}
	}
	printText(out, prefix, isLast, name, textui.Sprintf(
		"-> %q : %s",
		tgt,
		fmtInode(file.BareInode)))
}

func printFile(out io.Writer, prefix string, isLast bool, name string, file *btrfs.File) {
	if file.InodeItem != nil {
		if _, err := io.Copy(io.Discard, io.NewSectionReader(file, 0, file.InodeItem.Size)); err != nil {
			file.Errs = append(file.Errs, err)
		}
	}
	printText(out, prefix, isLast, name, fmtInode(file.BareInode))
}

func printSocket(out io.Writer, prefix string, isLast bool, name string, file *btrfs.File) {
	if file.InodeItem != nil && file.InodeItem.Size > 0 {
		panic(fmt.Errorf("TODO: I don't know how to handle a socket with size>0: %q", name))
	}
	printText(out, prefix, isLast, name, fmtInode(file.BareInode))
}

func printPipe(out io.Writer, prefix string, isLast bool, name string, file *btrfs.File) {
	if file.InodeItem != nil && file.InodeItem.Size > 0 {
		panic(fmt.Errorf("TODO: I don't know how to handle a pipe with size>0: %q", name))
	}
	printText(out, prefix, isLast, name, fmtInode(file.BareInode))
}
