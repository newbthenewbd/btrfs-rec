// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func init() {
	var nodescanFilename string
	cmd := subcommand{
		Command: cobra.Command{
			Use:   "ls-trees",
			Short: "A brief view what types of items are in each tree",
			Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) error {
			var scanResults map[btrfsvol.DeviceID]btrfsinspect.ScanOneDevResult
			if nodescanFilename != "" {
				scanResultsBytes, err := os.ReadFile(nodescanFilename)
				if err != nil {
					return err
				}
				if err := json.Unmarshal(scanResultsBytes, &scanResults); err != nil {
					return err
				}
			}

			var treeErrCnt int
			var treeItemCnt map[btrfsitem.Type]int
			flush := func() {
				totalItems := 0
				for _, cnt := range treeItemCnt {
					totalItems += cnt
				}
				numWidth := len(strconv.Itoa(slices.Max(treeErrCnt, totalItems)))

				table := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0)
				fmt.Fprintf(table, "        errors\t% *s\n", numWidth, strconv.Itoa(treeErrCnt))
				for _, typ := range maps.SortedKeys(treeItemCnt) {
					fmt.Fprintf(table, "        %v items\t% *s\n", typ, numWidth, strconv.Itoa(treeItemCnt[typ]))
				}
				fmt.Fprintf(table, "        total items\t% *s\n", numWidth, strconv.Itoa(totalItems))
				table.Flush()
			}
			visitedNodes := make(map[btrfsvol.LogicalAddr]struct{})
			btrfsutil.WalkAllTrees(cmd.Context(), fs, btrfsutil.WalkAllTreesHandler{
				PreTree: func(name string, treeID btrfs.ObjID) {
					treeErrCnt = 0
					treeItemCnt = make(map[btrfsitem.Type]int)
					fmt.Printf("tree id=%v name=%q\n", treeID, name)
				},
				Err: func(_ *btrfsutil.WalkError) {
					treeErrCnt++
				},
				TreeWalkHandler: btrfs.TreeWalkHandler{
					Node: func(_ btrfs.TreePath, ref *diskio.Ref[btrfsvol.LogicalAddr, btrfs.Node]) error {
						visitedNodes[ref.Addr] = struct{}{}
						return nil
					},
					Item: func(_ btrfs.TreePath, item btrfs.Item) error {
						typ := item.Key.ItemType
						treeItemCnt[typ] = treeItemCnt[typ] + 1
						return nil
					},
					BadItem: func(_ btrfs.TreePath, item btrfs.Item) error {
						typ := item.Key.ItemType
						treeItemCnt[typ] = treeItemCnt[typ] + 1
						return nil
					},
				},
				PostTree: func(_ string, _ btrfs.ObjID) {
					flush()
				},
			})

			if nodescanFilename != "" {
				treeErrCnt = 0
				treeItemCnt = make(map[btrfsitem.Type]int)
				fmt.Printf("lost+found\n")
				for _, devResults := range scanResults {
					for laddr := range devResults.FoundNodes {
						if _, visited := visitedNodes[laddr]; visited {
							continue
						}
						visitedNodes[laddr] = struct{}{}
						node, err := fs.ReadNode(laddr)
						if err != nil {
							treeErrCnt++
							continue
						}
						for _, item := range node.Data.BodyLeaf {
							typ := item.Key.ItemType
							treeItemCnt[typ] = treeItemCnt[typ] + 1
						}
					}
				}
				flush()
			}

			return nil
		},
	}
	cmd.Command.Flags().StringVar(&nodescanFilename, "nodescan", "", "Output of scan-for-nodes to use for a lost+found tree")
	if err := cmd.Command.MarkFlagFilename("nodescan"); err != nil {
		panic(err)
	}
	inspectors = append(inspectors, cmd)
}
