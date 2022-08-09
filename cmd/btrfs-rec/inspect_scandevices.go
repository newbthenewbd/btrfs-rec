// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"context"
	"os"
	"sync"

	"git.lukeshu.com/go/lowmemjson"
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
			buffer := bufio.NewWriter(os.Stdout)
			defer func() {
				maybeSetErr(buffer.Flush())
			}()
			return lowmemjson.Encode(&lowmemjson.ReEncoder{
				Out:    buffer,
				Indent: "\t",
			}, results)
		},
	})
}
