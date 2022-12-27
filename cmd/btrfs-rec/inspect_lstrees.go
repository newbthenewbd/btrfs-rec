// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func init() {
	var scandevicesFilename string
	cmd := subcommand{
		Command: cobra.Command{
			Use:   "ls-trees",
			Short: "A brief view what types of items are in each tree",
			Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()
			var scanResults btrfsinspect.ScanDevicesResult
			if scandevicesFilename != "" {
				var err error
				scanResults, err = readJSONFile[btrfsinspect.ScanDevicesResult](ctx, scandevicesFilename)
				if err != nil {
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
				textui.Fprintf(table, "        errors\t% *s\n", numWidth, strconv.Itoa(treeErrCnt))
				for _, typ := range maps.SortedKeys(treeItemCnt) {
					textui.Fprintf(table, "        %v items\t% *s\n", typ, numWidth, strconv.Itoa(treeItemCnt[typ]))
				}
				textui.Fprintf(table, "        total items\t% *s\n", numWidth, strconv.Itoa(totalItems))
				table.Flush()
			}
			visitedNodes := make(containers.Set[btrfsvol.LogicalAddr])
			btrfsutil.WalkAllTrees(ctx, fs, btrfsutil.WalkAllTreesHandler{
				PreTree: func(name string, treeID btrfsprim.ObjID) {
					treeErrCnt = 0
					treeItemCnt = make(map[btrfsitem.Type]int)
					textui.Fprintf(os.Stdout, "tree id=%v name=%q\n", treeID, name)
				},
				Err: func(_ *btrfsutil.WalkError) {
					treeErrCnt++
				},
				TreeWalkHandler: btrfstree.TreeWalkHandler{
					Node: func(_ btrfstree.TreePath, ref *diskio.Ref[btrfsvol.LogicalAddr, btrfstree.Node]) error {
						visitedNodes.Insert(ref.Addr)
						return nil
					},
					Item: func(_ btrfstree.TreePath, item btrfstree.Item) error {
						typ := item.Key.ItemType
						treeItemCnt[typ] = treeItemCnt[typ] + 1
						return nil
					},
					BadItem: func(_ btrfstree.TreePath, item btrfstree.Item) error {
						typ := item.Key.ItemType
						treeItemCnt[typ] = treeItemCnt[typ] + 1
						return nil
					},
				},
				PostTree: func(_ string, _ btrfsprim.ObjID) {
					flush()
				},
			})

			if scandevicesFilename != "" {
				treeErrCnt = 0
				treeItemCnt = make(map[btrfsitem.Type]int)
				textui.Fprintf(os.Stdout, "lost+found\n")
				sb, _ := fs.Superblock()
				for _, devResults := range scanResults {
					for laddr := range devResults.FoundNodes {
						if visitedNodes.Has(laddr) {
							continue
						}
						visitedNodes.Insert(laddr)
						node, err := btrfstree.ReadNode[btrfsvol.LogicalAddr](fs, *sb, laddr, btrfstree.NodeExpectations{
							LAddr: containers.Optional[btrfsvol.LogicalAddr]{OK: true, Val: laddr},
						})
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
	cmd.Command.Flags().StringVar(&scandevicesFilename, "scandevices", "", "Output of 'scandevices' to use for a lost+found tree")
	if err := cmd.Command.MarkFlagFilename("scandevices"); err != nil {
		panic(err)
	}
	inspectors = append(inspectors, cmd)
}
