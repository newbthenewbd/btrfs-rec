// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:   "ls-trees",
			Short: "A brief view what types of items are in each tree",
			Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, _ *cobra.Command, _ []string) error {
			var treeErrCnt int
			var treeItemCnt map[btrfsitem.Type]int
			btrfsutil.WalkAllTrees(fs, btrfsutil.WalkAllTreesHandler{
				PreTree: func(name string, treeID btrfs.ObjID) {
					treeErrCnt = 0
					treeItemCnt = make(map[btrfsitem.Type]int)
					fmt.Printf("tree id=%v name=%q\n", treeID, name)
				},
				Err: func(_ *btrfsutil.WalkError) {
					treeErrCnt++
				},
				TreeWalkHandler: btrfs.TreeWalkHandler{
					Item: func(_ btrfs.TreePath, item btrfs.Item) error {
						typ := item.Key.ItemType
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
		},
	})
}
