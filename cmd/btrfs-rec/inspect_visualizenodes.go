// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "visualize-nodes NODESCAN.json OUTPUT_DIR",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(2)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			nodeScanResults, err := readScanResults(args[0])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[0])

			return rebuildnodes.VisualizeNodes(ctx, args[1], fs, nodeScanResults)
		},
	})
}
