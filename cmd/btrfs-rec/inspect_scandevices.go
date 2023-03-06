// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"os"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/rebuildmappings"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func init() {
	inspectors.AddCommand(&cobra.Command{
		Use:  "scandevices",
		Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		RunE: runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, _ []string) (err error) {
			ctx := cmd.Context()

			results, err := rebuildmappings.ScanDevices(ctx, fs)
			if err != nil {
				return err
			}

			dlog.Info(ctx, "Writing scan results to stdout...")
			if err := writeJSONFile(os.Stdout, results, lowmemjson.ReEncoderConfig{
				Indent:                "\t",
				ForceTrailingNewlines: true,
				CompactIfUnder:        16, //nolint:gomnd // This is what looks nice.
			}); err != nil {
				return err
			}
			dlog.Info(ctx, "... done writing")

			return nil
		}),
	})
}

func readNodeList(ctx context.Context, filename string) ([]btrfsvol.LogicalAddr, error) {
	if filename == "" {
		return nil, nil
	}
	results, err := readJSONFile[rebuildmappings.ScanDevicesResult](ctx, filename)
	if err != nil {
		return nil, err
	}
	var cnt int
	for _, devResults := range results {
		cnt += len(devResults.FoundNodes)
	}
	set := make(containers.Set[btrfsvol.LogicalAddr], cnt)
	for _, devResults := range results {
		for laddr := range devResults.FoundNodes {
			set.Insert(laddr)
		}
	}
	return maps.SortedKeys(set), nil
}
