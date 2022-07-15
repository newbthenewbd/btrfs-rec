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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
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

			scanResultsBytes, err := os.ReadFile(args[0])
			if err != nil {
				return err
			}
			var scanResults map[btrfsvol.DeviceID]btrfsinspect.ScanOneDevResult
			if err := json.Unmarshal(scanResultsBytes, &scanResults); err != nil {
				return err
			}

			devices := fs.LV.PhysicalVolumes()
			for _, devID := range maps.SortedKeys(scanResults) {
				dev, ok := devices[devID]
				if !ok {
					return fmt.Errorf("device ID %v mentioned in %q is not part of the filesystem",
						devID, args[0])
				}
				dlog.Infof(ctx, "Rebuilding mappings from results on device %v...",
					dev.Name())
				scanResults[devID].AddToLV(ctx, fs, dev)
			}

			dlog.Infof(ctx, "Writing reconstructed mappings to stdout...")
			if err := writeMappingsJSON(os.Stdout, fs); err != nil {
				return err
			}

			return nil
		},
	})
}

func writeMappingsJSON(w io.Writer, fs *btrfs.FS) error {
	mappings := fs.LV.Mappings()
	if _, err := io.WriteString(w, "[\n"); err != nil {
		return err
	}
	for i, mapping := range mappings {
		suffix := ","
		if i == len(mappings)-1 {
			suffix = ""
		}
		bs, err := json.Marshal(mapping)
		if err != nil {
			return err
		}
		if _, err := fmt.Printf("  %s%s\n", bs, suffix); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(w, "]\n"); err != nil {
		return err
	}
	return nil
}
