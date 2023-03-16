// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func init() {
	var nodeListFilename string
	cmd := &cobra.Command{
		Use:   "ls-trees",
		Short: "A brief view what types of items are in each tree",
		Long: "" +
			"If no --node-list is given, then a slow sector-by-sector scan " +
			"will be used to find all lost+found nodes.",
		Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		RunE: runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			var nodeList []btrfsvol.LogicalAddr
			var err error
			if nodeListFilename != "" {
				nodeList, err = readJSONFile[[]btrfsvol.LogicalAddr](ctx, nodeListFilename)
			} else {
				nodeList, err = btrfsutil.ListNodes(ctx, fs)
			}
			if err != nil {
				return err
			}

			var treeErrCnt int
			var treeItemCnt map[btrfsitem.Type]int
			flush := func() {
				totalItems := 0
				for _, cnt := range treeItemCnt {
					totalItems += cnt
				}
				numWidth := len(strconv.Itoa(slices.Max(treeErrCnt, totalItems)))

				table := tabwriter.NewWriter(os.Stdout, 0, 8, 2, ' ', 0) //nolint:gomnd // This is what looks nice.
				textui.Fprintf(table, "        errors\t% *s\n", numWidth, strconv.Itoa(treeErrCnt))
				for _, typ := range maps.SortedKeys(treeItemCnt) {
					textui.Fprintf(table, "        %v items\t% *s\n", typ, numWidth, strconv.Itoa(treeItemCnt[typ]))
				}
				textui.Fprintf(table, "        total items\t% *s\n", numWidth, strconv.Itoa(totalItems))
				_ = table.Flush()
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
					Node: func(path btrfstree.TreePath, node *btrfstree.Node) error {
						visitedNodes.Insert(path.Node(-1).ToNodeAddr)
						return nil
					},
					Item: func(_ btrfstree.TreePath, item btrfstree.Item) error {
						typ := item.Key.ItemType
						treeItemCnt[typ]++
						return nil
					},
					BadItem: func(_ btrfstree.TreePath, item btrfstree.Item) error {
						typ := item.Key.ItemType
						treeItemCnt[typ]++
						return nil
					},
				},
				PostTree: func(_ string, _ btrfsprim.ObjID) {
					flush()
				},
			})

			{
				treeErrCnt = 0
				treeItemCnt = make(map[btrfsitem.Type]int)
				textui.Fprintf(os.Stdout, "lost+found\n")
				sb, _ := fs.Superblock()
				for _, laddr := range nodeList {
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
					for _, item := range node.BodyLeaf {
						typ := item.Key.ItemType
						treeItemCnt[typ]++
					}
				}
				flush()
			}

			return nil
		}),
	}
	cmd.Flags().StringVar(&nodeListFilename, "node-list", "",
		"Output of 'btrfs-recs inspect [rebuild-mappings] list-nodes' to use for a lost+found tree")
	noError(cmd.MarkFlagFilename("node-list"))

	inspectors.AddCommand(cmd)
}
