// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"io"
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
			ctx := cmd.Context()

			results, err := btrfsinspect.ScanDevices(ctx, fs)
			if err != nil {
				return err
			}

			dlog.Info(ctx, "Writing scan results to stdout...")
			if err := writeScanResults(os.Stdout, results); err != nil {
				return err
			}
			dlog.Info(ctx, "... done writing")

			return nil
		},
	})
}

func writeScanResults(w io.Writer, results btrfsinspect.ScanDevicesResult) (err error) {
	buffer := bufio.NewWriter(w)
	defer func() {
		if _err := buffer.Flush(); err == nil && _err != nil {
			err = _err
		}
	}()
	return lowmemjson.Encode(&lowmemjson.ReEncoder{
		Out: buffer,

		Indent:                "\t",
		ForceTrailingNewlines: true,
		CompactIfUnder:        16,
	}, results)
}
