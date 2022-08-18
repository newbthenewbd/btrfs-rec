// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"os"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "scandevices",
			Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) (err error) {
			maybeSetErr := func(_err error) {
				if err == nil && _err != nil {
					err = _err
				}
			}
			ctx := cmd.Context()

			results, err := btrfsinspect.ScanDevices(ctx, fs)
			if err != nil {
				return err
			}

			dlog.Info(ctx, "Writing scan results to stdout...")
			buffer := bufio.NewWriter(os.Stdout)
			defer func() {
				maybeSetErr(buffer.Flush())
			}()
			return lowmemjson.Encode(&lowmemjson.ReEncoder{
				Out: buffer,

				Indent:                "\t",
				ForceTrailingNewlines: true,
				CompactIfUnder:        16,
			}, results)
		},
	})
}
