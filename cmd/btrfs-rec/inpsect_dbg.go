// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"fmt"
	"os"

	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/scanforextents"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "dbg DUMPSUMS.gob",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			sums, err := scanforextents.ReadAllSums(args[0])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[0])

			out := bufio.NewWriter(os.Stdout)

			dlog.Info(ctx, "Walking sum tree...")
			btrfsutil.NewBrokenTrees(ctx, fs).TreeWalk(ctx, btrfs.CSUM_TREE_OBJECTID,
				func(err *btrfs.TreeError) {
					dlog.Error(ctx, err)
				},
				btrfs.TreeWalkHandler{
					Item: func(path btrfs.TreePath, item btrfs.Item) error {
						if item.Key.ItemType != btrfsitem.EXTENT_CSUM_KEY {
							return nil
						}
						body := item.Body.(btrfsitem.ExtentCSum)

						for i, sum := range body.Sums {
							laddr := btrfsvol.LogicalAddr(item.Key.Offset) + (btrfsvol.LogicalAddr(i) * scanforextents.CSumBlockSize)
							fmt.Fprintf(out, "walk: %v=%q\n",
								laddr, scanforextents.ShortSum(sum[:body.ChecksumSize]))
						}
						return nil
					},
				},
			)

			dlog.Info(ctx, "Dumping gob...")
			_ = sums.WalkLogical(ctx, func(laddr btrfsvol.LogicalAddr, sum scanforextents.ShortSum) error {
				fmt.Fprintf(out, "gob: %v=%q\n",
					laddr, sum)
				return nil
			})

			return out.Flush()
		},
	})
}
