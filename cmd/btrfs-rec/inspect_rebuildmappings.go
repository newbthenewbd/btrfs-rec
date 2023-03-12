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

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/rebuildmappings"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:   "rebuild-mappings SCAN_RESULT.json",
			Short: "Rebuild broken chunk/dev-extent/blockgroup trees",
			Long: "" +
				"The rebuilt information is printed as JSON on stdout, and can\n" +
				"be loaded by the --mappings flag.\n" +
				"\n" +
				"This is very similar to `btrfs rescue chunk-recover`, but (1)\n" +
				"does a better job, (2) is less buggy, and (3) doesn't actually\n" +
				"write the info back to the filesystem; instead writing it\n" +
				"out-of-band to stdout.",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			scanResults, err := readJSONFile[rebuildmappings.ScanDevicesResult](ctx, args[0])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[0])

			if err := rebuildmappings.RebuildMappings(ctx, fs, scanResults); err != nil {
				return err
			}

			dlog.Infof(ctx, "Writing reconstructed mappings to stdout...")
			if err := writeJSONFile(os.Stdout, fs.LV.Mappings(), lowmemjson.ReEncoderConfig{
				Indent:                "\t",
				ForceTrailingNewlines: true,
				CompactIfUnder:        120, //nolint:gomnd // This is what looks nice.
			}); err != nil {
				return err
			}
			dlog.Info(ctx, "... done writing")

			return nil
		},
	})
}
