// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"os"
	"runtime"

	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildmappings"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "scan-for-extents NODESCAN.json DUMPSUMS.gob",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(2)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			bgs, err := rebuildmappings.ReadNodeScanResults(fs, args[0])
			if err != nil {
				return err
			}
			runtime.GC()
			dlog.Infof(ctx, "... done reading %q", args[0])

			dlog.Infof(ctx, "Reading %q...", args[1])
			sums, err := btrfsinspect.ReadAllSums(args[1])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[1])

			if err := rebuildmappings.ScanForExtents(ctx, fs, bgs, sums); err != nil {
				return err
			}

			dlog.Infof(ctx, "Writing reconstructed mappings to stdout...")
			if err := writeMappingsJSON(os.Stdout, fs); err != nil {
				return err
			}

			return nil
		},
	})
}
