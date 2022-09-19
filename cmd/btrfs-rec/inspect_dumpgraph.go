// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"os"

	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "show-loops NODESCAN.json",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) (err error) {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			nodeScanResults, err := readScanResults(args[0])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[0])

			buffer := bufio.NewWriter(os.Stdout)
			defer func() {
				if _err := buffer.Flush(); err == nil && _err != nil {
					err = _err
				}
			}()
			return rebuildnodes.ShowLoops(ctx, buffer, fs, nodeScanResults)
		},
	})
}
