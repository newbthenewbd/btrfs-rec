// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:   "recover-chunks",
			Short: "Rebuild broken chunk/dev-extent/blockgroup trees",
			Long: "" +
				"The rebuilt information is printed as JSON on stdout, and can\n" +
				"be loaded by the --mappings flag.\n" +
				"\n" +
				"This is very similar to `btrfs rescue chunk-recover`, but (1)\n" +
				"does a better job, (2) is less buggy, and (3) doesn't actually\n" +
				"write the info back to the filesystem; instead writing it\n" +
				"out-of-band to stdout.",
			Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			dlog.Info(ctx, "Reading superblock...")
			superblock, err := fs.Superblock()
			if err != nil {
				return err
			}

			for _, dev := range fs.LV.PhysicalVolumes() {
				dlog.Infof(ctx, "dev[%q] Scanning for unreachable nodes...", dev.Name())
				devResult, err := btrfsinspect.ScanOneDev(ctx, dev, superblock.Data)
				if err != nil {
					return err
				}

				dlog.Infof(ctx, "dev[%q] Re-inserting lost+found mappings...", dev.Name())
				devResult.AddToLV(ctx, fs, dev)
			}

			dlog.Infof(ctx, "Writing reconstructed mappings to stdout...")

			mappings := fs.LV.Mappings()
			_, _ = io.WriteString(os.Stdout, "{\n  \"Mappings\": [\n")
			for i, mapping := range mappings {
				suffix := ","
				if i == len(mappings)-1 {
					suffix = ""
				}
				bs, err := json.Marshal(mapping)
				if err != nil {
					return err
				}
				fmt.Printf("    %s%s\n", bs, suffix)
			}
			_, _ = io.WriteString(os.Stdout, "  ]\n}\n")
			return nil
		},
	})
}
