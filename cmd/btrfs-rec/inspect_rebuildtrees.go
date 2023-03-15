// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"os"
	"runtime"
	"time"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/rebuildtrees"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func init() {
	var nodeListFilename string
	cmd := &cobra.Command{
		Use: "rebuild-trees",
		Long: "" +
			"Rebuild broken btrees based on missing items that are implied " +
			"by present items.  This requires functioning " +
			"chunk/dev-extent/blockgroup trees, which can be rebuilt " +
			"separately with `btrfs-rec inspect rebuild-mappings`.\n" +
			"\n" +
			"If no --node-list is given, then a slow sector-by-sector scan " +
			"will be used to find all nodes.",
		Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		RunE: runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
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

			rebuilder, err := rebuildtrees.NewRebuilder(ctx, fs, nodeList)
			if err != nil {
				return err
			}

			runtime.GC()
			time.Sleep(textui.LiveMemUseUpdateInterval) // let the logs reflect that GC right away

			dlog.Info(ctx, "Rebuilding node tree...")
			rebuildErr := rebuilder.Rebuild(ctx)
			dst := os.Stdout
			if rebuildErr != nil {
				dst = os.Stderr
				dlog.Errorf(ctx, "rebuild error: %v", rebuildErr)
			}
			dlog.Infof(ctx, "Writing re-built nodes to %s...", dst.Name())
			if err := writeJSONFile(dst, rebuilder.ListRoots(ctx), lowmemjson.ReEncoderConfig{
				Indent:                "\t",
				CompactIfUnder:        80, //nolint:gomnd // This is what looks nice.
				ForceTrailingNewlines: true,
			}); err != nil {
				if rebuildErr != nil {
					return rebuildErr
				}
				return err
			}
			dlog.Info(ctx, "... done writing")

			return rebuildErr
		}),
	}
	cmd.Flags().StringVar(&nodeListFilename, "node-list", "",
		"Output of 'btrfs-recs inspect [rebuild-mappings] list-nodes' to use for the node list")
	noError(cmd.MarkFlagFilename("node-list"))

	inspectors.AddCommand(cmd)
}
