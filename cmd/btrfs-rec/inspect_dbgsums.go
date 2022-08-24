// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "dbgsums SCAN_RESULT.json",
			Args: cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) (err error) {
			maybeSetErr := func(_err error) {
				if err == nil && _err != nil {
					err = _err
				}
			}
			ctx := cmd.Context()

			dlog.Infof(ctx, "Reading %q...", args[0])
			scanResults, err := readScanResults(args[0])
			if err != nil {
				return err
			}
			dlog.Infof(ctx, "... done reading %q", args[0])

			dlog.Info(ctx, "Mapping the logical address space...")
			type record struct {
				Gen btrfs.Generation
				Sum btrfssum.ShortSum
			}
			addrspace := make(map[btrfsvol.LogicalAddr]record)
			var sumSize int
			for _, devResults := range scanResults {
				sumSize = devResults.Checksums.ChecksumSize
				for _, sumItem := range devResults.FoundExtentCSums {
					_ = sumItem.Sums.Walk(ctx, func(pos btrfsvol.LogicalAddr, sum btrfssum.ShortSum) error {
						new := record{
							Gen: sumItem.Generation,
							Sum: sum,
						}
						if old, ok := addrspace[pos]; ok {
							switch {
							case old.Gen > new.Gen:
								// do nothing
							case old.Gen < new.Gen:
								addrspace[pos] = new
							case old.Gen == new.Gen:
								if old != new {
									dlog.Errorf(ctx, "mismatch of laddr=%v sum: %v != %v", pos, old, new)
								}
							}
						} else {
							addrspace[pos] = new
						}
						return nil
					})
				}
			}
			if len(addrspace) == 0 {
				return fmt.Errorf("no checksums were found in the scan")
			}
			dlog.Info(ctx, "... done mapping")

			dlog.Info(ctx, "Flattening the map ...")
			var flattened btrfssum.SumRunWithGaps[btrfsvol.LogicalAddr]
			var curAddr btrfsvol.LogicalAddr
			var curSums strings.Builder
			for _, laddr := range maps.SortedKeys(addrspace) {
				if laddr != curAddr+(btrfsvol.LogicalAddr(curSums.Len()/sumSize)*btrfssum.BlockSize) {
					if curSums.Len() > 0 {
						flattened.Runs = append(flattened.Runs, btrfssum.SumRun[btrfsvol.LogicalAddr]{
							ChecksumSize: sumSize,
							Addr:         curAddr,
							Sums:         btrfssum.ShortSum(curSums.String()),
						})
					}
					curAddr = laddr
					curSums.Reset()
				}
				curSums.WriteString(string(addrspace[laddr].Sum))
			}
			if curSums.Len() > 0 {
				flattened.Runs = append(flattened.Runs, btrfssum.SumRun[btrfsvol.LogicalAddr]{
					ChecksumSize: sumSize,
					Addr:         curAddr,
					Sums:         btrfssum.ShortSum(curSums.String()),
				})
			}
			flattened.Addr = flattened.Runs[0].Addr
			last := flattened.Runs[len(flattened.Runs)-1]
			end := last.Addr.Add(last.Size())
			flattened.Size = end.Sub(flattened.Addr)
			dlog.Info(ctx, "... done flattening")

			dlog.Info(ctx, "Writing addrspace sums to stdout...")
			buffer := bufio.NewWriter(os.Stdout)
			defer func() {
				maybeSetErr(buffer.Flush())
			}()
			return lowmemjson.Encode(&lowmemjson.ReEncoder{
				Out: buffer,

				Indent:                "\t",
				ForceTrailingNewlines: true,
			}, flattened)
		},
	})
}
