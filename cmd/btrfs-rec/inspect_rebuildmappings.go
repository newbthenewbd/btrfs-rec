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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func init() {
	cmd := &cobra.Command{
		Use:   "rebuild-mappings",
		Short: "Rebuild broken chunk/dev/blockgroup trees",
		Long: "" +
			"The rebuilt information is printed as JSON on stdout, and can " +
			"be loaded by the --mappings flag.\n" +
			"\n" +
			"This is very similar to `btrfs rescue chunk-recover`, but (1) " +
			"does a better job, (2) is less buggy, and (3) doesn't actually " +
			"write the info back to the filesystem; instead writing it " +
			"out-of-band to stdout.\n" +
			"\n" +
			"The I/O and the CPU parts of this can be split up as:\n" +
			"\n" +
			"\tbtrfs-rec inspect rebuild-mappings scan > SCAN.json   # read\n" +
			"\tbtrfs-rec inspect rebuild-mappings process SCAN.json  # CPU\n",
		Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		RunE: runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			scanResults, err := rebuildmappings.ScanDevices(ctx, fs)
			if err != nil {
				return err
			}

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
		}),
	}

	cmd.AddCommand(&cobra.Command{
		Use:   "scan",
		Short: "Read from the filesystem all data nescessary to rebuild the mappings",
		Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		RunE: runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, _ []string) (err error) {
			ctx := cmd.Context()

			scanResults, err := rebuildmappings.ScanDevices(ctx, fs)
			if err != nil {
				return err
			}

			dlog.Info(ctx, "Writing scan results to stdout...")
			if err := writeJSONFile(os.Stdout, scanResults, lowmemjson.ReEncoderConfig{
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

	cmd.AddCommand(&cobra.Command{
		Use:   "process",
		Short: "Rebuild the mappings based on previously read data",
		Args:  cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		RunE: runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
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
		}),
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "list-nodes",
		Short: "Produce a listing of btree nodes from previously read data",
		Long: "" +
			"This is a variant of `btrfs-rec inspect list-nodes` that takes " +
			"advantage of using previously read data from " +
			"`btrfs-rec inspect rebuild-nodes scan`.",
		Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		RunE: run(func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()

			scanResults, err := readJSONFile[rebuildmappings.ScanDevicesResult](ctx, args[0])
			if err != nil {
				return err
			}

			var cnt int
			for _, devResults := range scanResults {
				cnt += len(devResults.FoundNodes)
			}
			set := make(containers.Set[btrfsvol.LogicalAddr], cnt)
			for _, devResults := range scanResults {
				for laddr := range devResults.FoundNodes {
					set.Insert(laddr)
				}
			}
			nodeList := maps.SortedKeys(set)

			dlog.Infof(ctx, "Writing nodes to stdout...")
			if err := writeJSONFile(os.Stdout, nodeList, lowmemjson.ReEncoderConfig{
				Indent:                "\t",
				ForceTrailingNewlines: true,
			}); err != nil {
				return err
			}
			dlog.Info(ctx, "... done writing")

			return nil
		}),
	})

	inspectors.AddCommand(cmd)
}
