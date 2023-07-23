// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"os"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
)

func init() {
	inspectors.AddCommand(&cobra.Command{
		Use:   "list-nodes",
		Short: "Scan the filesystem for btree nodes",
		Long: "" +
			"This scans the filesystem sector-by-sector looking for nodes.  " +
			"If you are needing to rebuild the chunk/dev-extent/blockgroup " +
			"trees with `btrfs-rec inspect rebuild-mappings` anyway, you may " +
			"want to instead use `btrfs-rec inspect rebuild-mappings list-nodes` " +
			"to take advantage of the sector-by-sector scan that's already " +
			"performed by `btrfs-rec inspect rebuild-mappings scan`.",
		Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		RunE: runWithRawFS(nil, func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			nodeList, err := btrfsutil.ListNodes(ctx, fs)
			if err != nil {
				return err
			}

			dlog.Infof(ctx, "Writing nodes to stdout...")
			if err := writeJSONFile(os.Stdout, nodeList, lowmemjson.ReEncoderConfig{
				Indent:                "\t",
				ForceTrailingNewlines: true,
			}); err != nil {
				return err
			}
			dlog.Info(ctx, "... done writing")

			return nil
		}),
	})
}
