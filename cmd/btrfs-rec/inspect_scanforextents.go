// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"os"
	"runtime"
	"sort"
	"sync"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"
	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
)

const csumBlockSize = 4 * 1024

type shortSum string

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:  "scan-for-extents",
			Args: cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) error {
			ctx := cmd.Context()

			dlog.Info(ctx, "Reading checksum tree...")
			sum2laddrs := listUnmappedCheckummedExtents(ctx, fs)
			dlog.Info(ctx, "... done reading checksum tree")

			dlog.Info(ctx, "Pruning duplicate sums...")
			for sum, addrs := range sum2laddrs {
				if len(addrs) != 1 {
					delete(sum2laddrs, sum)
				}
			}
			dlog.Info(ctx, "... done pruning")

			devs := fs.LV.PhysicalVolumes()
			gaps := listPhysicalGaps(fs)

			var mu sync.Mutex
			sum2paddrs := make(map[shortSum][]btrfsvol.QualifiedPhysicalAddr)
			grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
			for devID := range gaps {
				devGaps := gaps[devID]
				dev := devs[devID]
				grp.Go(dev.Name(), func(ctx context.Context) error {
					devSum2paddrs, err := scanOneDev(ctx, dev, devGaps, sum2laddrs)
					mu.Lock()
					for sum, paddrs := range devSum2paddrs {
						sum2paddrs[sum] = append(sum2paddrs[sum], paddrs...)
					}
					mu.Unlock()
					return err
				})
			}
			if err := grp.Wait(); err != nil {
				return err
			}

			dlog.Info(ctx, "Rebuilding mappings from results...")
			rebuildMappings(ctx, fs, devs, sum2laddrs, sum2paddrs)
			dlog.Info(ctx, "... done rebuilding mappings")

			dlog.Infof(ctx, "Writing reconstructed mappings to stdout...")
			if err := writeMappingsJSON(os.Stdout, fs); err != nil {
				return err
			}

			return nil
		},
	})
}

func listUnmappedCheckummedExtents(ctx context.Context, fs *btrfs.FS) map[shortSum][]btrfsvol.LogicalAddr {
	sum2laddrs := make(map[shortSum][]btrfsvol.LogicalAddr)
	btrfsutil.NewBrokenTrees(ctx, fs).TreeWalk(ctx, btrfs.CSUM_TREE_OBJECTID,
		func(err *btrfs.TreeError) {
			dlog.Error(ctx, err)
		},
		btrfs.TreeWalkHandler{
			Item: func(path btrfs.TreePath, item btrfs.Item) error {
				if item.Key.ItemType != btrfsitem.EXTENT_CSUM_KEY {
					return nil
				}
				body := item.Body.(btrfsitem.ExtentCSum)
				for i, _sum := range body.Sums {
					laddr := btrfsvol.LogicalAddr(item.Key.Offset) + btrfsvol.LogicalAddr(i*csumBlockSize)
					if paddrs, _ := fs.LV.Resolve(laddr); len(paddrs) > 0 {
						continue
					}
					sum := shortSum(_sum[:body.ChecksumSize])
					sum2laddrs[sum] = append(sum2laddrs[sum], laddr)
				}
				return nil
			},
		},
	)
	return sum2laddrs
}

type physicalGap struct {
	Beg, End btrfsvol.PhysicalAddr
}

func listPhysicalGaps(fs *btrfs.FS) map[btrfsvol.DeviceID][]physicalGap {
	gaps := make(map[btrfsvol.DeviceID][]physicalGap)
	pos := make(map[btrfsvol.DeviceID]btrfsvol.PhysicalAddr)
	mappings := fs.LV.Mappings()
	sort.Slice(mappings, func(i, j int) bool {
		return mappings[i].PAddr.Cmp(mappings[j].PAddr) < 0
	})
	for _, mapping := range mappings {
		if pos[mapping.PAddr.Dev] < mapping.PAddr.Addr {
			gaps[mapping.PAddr.Dev] = append(gaps[mapping.PAddr.Dev], physicalGap{
				Beg: pos[mapping.PAddr.Dev],
				End: mapping.PAddr.Addr,
			})
		}
		if pos[mapping.PAddr.Dev] < mapping.PAddr.Addr.Add(mapping.Size) {
			pos[mapping.PAddr.Dev] = mapping.PAddr.Addr.Add(mapping.Size)
		}
	}
	for devID, dev := range fs.LV.PhysicalVolumes() {
		devSize := dev.Size()
		if pos[devID] < devSize {
			gaps[devID] = append(gaps[devID], physicalGap{
				Beg: pos[devID],
				End: devSize,
			})
		}
	}
	return gaps
}

func roundUp[T constraints.Integer](x, multiple T) T {
	return ((x + multiple - 1) / multiple) * multiple
}

func scanOneDev[T any](ctx context.Context, dev *btrfs.Device, gaps []physicalGap, sumsToScanFor map[shortSum]T) (map[shortSum][]btrfsvol.QualifiedPhysicalAddr, error) {
	dlog.Infof(ctx, "... dev[%q] Scanning for extents...", dev.Name())
	sb, err := dev.Superblock()
	if err != nil {
		return nil, err
	}

	var totalBlocks int64
	for _, gap := range gaps {
		for paddr := roundUp(gap.Beg, csumBlockSize); paddr+csumBlockSize <= gap.End; paddr += csumBlockSize {
			totalBlocks++
		}
	}

	lastProgress := -1
	var curBlock int64
	progress := func() {
		pct := int(100 * float64(curBlock) / float64(totalBlocks))
		if pct != lastProgress || curBlock == totalBlocks {
			dlog.Infof(ctx, "... dev[%q] scanned %v%%",
				dev.Name(), pct)
			lastProgress = pct
			if pct%5 == 0 {
				runtime.GC()
			}
		}
	}

	sumSize := sb.ChecksumType.Size()

	var buf [csumBlockSize]byte
	devSum2paddrs := make(map[shortSum][]btrfsvol.QualifiedPhysicalAddr)
	for _, gap := range gaps {
		for paddr := roundUp(gap.Beg, csumBlockSize); paddr+csumBlockSize <= gap.End; paddr += csumBlockSize {
			progress()
			curBlock++
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			_, err := dev.ReadAt(buf[:], paddr)
			if err != nil {
				dlog.Error(ctx, err)
				continue
			}
			_sum, err := sb.ChecksumType.Sum(buf[:])
			if err != nil {
				dlog.Error(ctx, err)
				continue
			}
			sum := shortSum(_sum[:sumSize])
			if _, interesting := sumsToScanFor[sum]; !interesting {
				continue
			}
			devSum2paddrs[sum] = append(devSum2paddrs[sum], btrfsvol.QualifiedPhysicalAddr{
				Dev:  sb.DevItem.DevID,
				Addr: paddr,
			})
		}
	}
	progress()
	return devSum2paddrs, nil
}

func rebuildMappings(ctx context.Context, fs *btrfs.FS,
	devs map[btrfsvol.DeviceID]*btrfs.Device,
	sum2laddrs map[shortSum][]btrfsvol.LogicalAddr,
	sum2paddrs map[shortSum][]btrfsvol.QualifiedPhysicalAddr) {

	totalPairs := len(sum2paddrs)
	lastProgress := -1
	var donePairs int
	progress := func() {
		pct := int(100 * float64(donePairs) / float64(totalPairs))
		if pct != lastProgress || donePairs == totalPairs {
			dlog.Infof(ctx, "... rebuilt %v%% (%v/%v)", pct, donePairs, totalPairs)
			lastProgress = pct
			if pct%5 == 0 {
				runtime.GC()
			}
		}
	}

	for sum, paddrs := range sum2paddrs {
		progress()
		if len(paddrs) == 1 {
			mapping := btrfsvol.Mapping{
				LAddr: sum2laddrs[sum][0],
				PAddr: paddrs[0],
				Size:  csumBlockSize,
			}
			if err := fs.LV.AddMapping(mapping); err != nil {
				dlog.Errorf(ctx, "... dev[%q] error: adding chunk: %v",
					devs[paddrs[0].Dev].Name(), err)
			}
		} else {
			delete(sum2paddrs, sum)
			delete(sum2laddrs, sum)
		}
		donePairs++

	}
	progress()
}
