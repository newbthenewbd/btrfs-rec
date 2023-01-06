// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"os"
	"runtime"
	"time"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "rebuild-nodes NODESCAN.json",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			// This is wrapped in a func in order to *ensure* that `nodeScanResults` goes out of scope once
			// `rebuilder` has been created.
			rebuilder, err := func(ctx context.Context) (rebuildnodes.Rebuilder, error) {
				dlog.Infof(ctx, "Reading %q...", args[0])
				nodeScanResults, err := readJSONFile[btrfsinspect.ScanDevicesResult](ctx, args[0])
				if err != nil {
					return nil, err
				}
				dlog.Infof(ctx, "... done reading %q", args[0])

				return rebuildnodes.NewRebuilder(ctx, fs, nodeScanResults)
			}(ctx)
			if err != nil {
				return err
			}

			runtime.GC()
			time.Sleep(textui.LiveMemUseUpdateInterval) // let the logs reflect that GC right away

			dlog.Info(ctx, "Rebuilding node tree...")
			rebuildErr := rebuilder.Rebuild(ctx)
			dst := os.Stdout
			if rebuildErr != nil {
				dst = os.Stderr
				dlog.Errorf(ctx, "rebuild error: %v", rebuildErr)
			}
			dlog.Infof(ctx, "Writing re-built nodes to %s...", dst.Name())
			if err := writeJSONFile(dst, rebuilder.ListRoots(), lowmemjson.ReEncoderConfig{
				Indent:                "\t",
				CompactIfUnder:        80, //nolint:gomnd // This is what looks nice.
				ForceTrailingNewlines: true,
			}); err != nil {
				if rebuildErr != nil {
					return rebuildErr
				}
				return err
			}
			dlog.Info(ctx, "... done writing")

			return rebuildErr
		},
	})
}
