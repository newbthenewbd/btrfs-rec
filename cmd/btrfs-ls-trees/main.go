// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

func main() {
	if err := Main(os.Args[1:]...); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilenames ...string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fs, err := btrfsutil.Open(os.O_RDONLY, imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	var treeErrCnt int
	var treeItemCnt map[btrfsitem.Type]int
	btrfsutil.WalkAllTrees(fs, btrfsutil.WalkAllTreesHandler{
		PreTree: func(name string, treeID btrfs.ObjID) {
			treeErrCnt = 0
			treeItemCnt = make(map[btrfsitem.Type]int)
			fmt.Printf("tree id=%v name=%q\n", treeID, name)
		},
		Err: func(_ error) {
			treeErrCnt++
		},
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Item: func(_ btrfs.TreePath, item btrfs.Item) error {
				typ := item.Head.Key.ItemType
				treeItemCnt[typ] = treeItemCnt[typ] + 1
				return nil
			},
		},
		PostTree: func(_ string, _ btrfs.ObjID) {
			totalItems := 0
			for _, cnt := range treeItemCnt {
				totalItems += cnt
			}
			numWidth := len(strconv.Itoa(util.Max(treeErrCnt, totalItems)))

			table := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
			fmt.Fprintf(table, "        errors\t% *s\n", numWidth, strconv.Itoa(treeErrCnt))
			for _, typ := range util.SortedMapKeys(treeItemCnt) {
				fmt.Fprintf(table, "        %v items\t% *s\n", typ, numWidth, strconv.Itoa(treeItemCnt[typ]))
			}
			fmt.Fprintf(table, "        total items\t% *s\n", numWidth, strconv.Itoa(totalItems))
			table.Flush()
		},
	})

	return nil
}
