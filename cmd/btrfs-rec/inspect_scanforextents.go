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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/scanforextents"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "scan-for-extents SCAN_RESULT.json",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			bgs, err := scanforextents.ReadNodeScanResults(fs, args[0])
			if err != nil {
				return err
			}
			runtime.GC()
			dlog.Infof(ctx, "... done reading %q", args[0])

			if err := scanforextents.ScanForExtents(ctx, fs, bgs); err != nil {
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
