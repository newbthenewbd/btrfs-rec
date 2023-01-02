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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "rebuild-nodes NODESCAN.json",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			nodeScanResults, err := readJSONFile[btrfsinspect.ScanDevicesResult](ctx, args[0])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[0])

			rebuiltNodes, err := rebuildnodes.RebuildNodes(ctx, fs, nodeScanResults)
			if err != nil {
				return err
			}

			dlog.Info(ctx, "Writing re-built nodes to stdout...")
			if err := writeJSONFile(os.Stdout, rebuiltNodes, lowmemjson.ReEncoder{
				Indent:                "\t",
				ForceTrailingNewlines: true,
			}); err != nil {
				return err
			}
			dlog.Info(ctx, "... done writing")

			return nil
		},
	})
}
