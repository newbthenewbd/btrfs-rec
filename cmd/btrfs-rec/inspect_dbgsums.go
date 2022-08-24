// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"fmt"
	"os"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildmappings"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "dbgsums SCAN_RESULT.json",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) (err error) {
			maybeSetErr := func(_err error) {
				if err == nil && _err != nil {
					err = _err
				}
			}
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			scanResults, err := readScanResults(args[0])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[0])

			flattened := rebuildmappings.ExtractLogicalSums(ctx, scanResults)
			if len(flattened.Runs) == 0 {
				return fmt.Errorf("no checksums were found in the scan")
			}

			dlog.Info(ctx, "Writing addrspace sums to stdout...")
			buffer := bufio.NewWriter(os.Stdout)
			defer func() {
				maybeSetErr(buffer.Flush())
			}()
			return lowmemjson.Encode(&lowmemjson.ReEncoder{
				Out: buffer,

				Indent:                "\t",
				ForceTrailingNewlines: true,
			}, flattened)
		},
	})
}
