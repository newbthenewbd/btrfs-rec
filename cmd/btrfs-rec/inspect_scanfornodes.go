// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"encoding/json"
	"os"
	"sync"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:   "scan-for-nodes",
			Short: "Scan devices for (potentially lost) nodes",
			Long: "" +
				"The found information is printed as JSON on stdout, and can\n" +
				"be read by `btrfs-rec inspect rebuild-mappings`.\n" +
				"\n" +
				"This information is mostly useful for rebuilding a broken\n" +
				"chunk/dev-extent/blockgroup trees, but can also have some\n" +
				"minimal utility in repairing other trees.\n" +
				"\n" +
				"This is very similar the initial scan done by\n" +
				"`btrfs rescue chunk-recover`.  Like `btrfs rescue chunk-recover`,\n" +
				"this is likely probably slow because it reads the entirety of\n" +
				"each device.",
			Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			var resultsMu sync.Mutex
			results := make(map[btrfsvol.DeviceID]btrfsinspect.ScanOneDeviceResult)
			grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
			for _, dev := range fs.LV.PhysicalVolumes() {
				dev := dev
				grp.Go(dev.Name(), func(ctx context.Context) error {
					superblock, err := dev.Superblock()
					if err != nil {
						return err
					}
					dlog.Infof(ctx, "dev[%q] Scanning for unreachable nodes...", dev.Name())
					devResult, err := btrfsinspect.ScanOneDevice(ctx, dev, *superblock)
					dlog.Infof(ctx, "dev[%q] Finished scanning", dev.Name())
					resultsMu.Lock()
					results[superblock.DevItem.DevID] = devResult
					resultsMu.Unlock()
					return err
				})
			}
			if err := grp.Wait(); err != nil {
				return err
			}

			dlog.Info(ctx, "Writing scan results to stdout...")
			return json.NewEncoder(os.Stdout).Encode(results)
		},
	})
}
