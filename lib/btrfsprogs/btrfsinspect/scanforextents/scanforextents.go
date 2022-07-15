// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package scanforextents

import (
	"context"
	"sync"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

func ScanForExtents(ctx context.Context, fs *btrfs.FS, blockGroups *BlockGroupTree) error {
	treeReader := btrfsutil.NewBrokenTrees(ctx, fs)

	dlog.Info(ctx, "Reading checksum tree...")
	sum2laddrs := readCSumTree(ctx, treeReader)
	if len(sum2laddrs) == 0 {
		dlog.Info(ctx, "No unmapped checksums")
		return nil
	}

	devs := fs.LV.PhysicalVolumes()
	gaps := ListPhysicalGaps(fs)

	newMappings := &ExtentMappings{
		InFS:          treeReader,
		InLV:          &fs.LV,
		InSum2laddrs:  sum2laddrs,
		InBlockGroups: blockGroups,
	}

	grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{})
	for devID := range gaps {
		dev := devs[devID]
		devGaps := gaps[devID]
		grp.Go(dev.Name(), func(ctx context.Context) error {
			return newMappings.ScanOneDev(ctx, dev, devGaps)
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}

	for laddr, mappings := range newMappings.OutSum2mappings {
		if len(mappings) > 1 {
			dlog.Errorf(ctx, "multiple possibilities for laddr=%v :", laddr)
			for _, mapping := range mappings {
				dlog.Errorf(ctx, "  - %#v", *mapping)
			}
			continue
		}
		if err := fs.LV.AddMapping(*mappings[0]); err != nil {
			dlog.Error(ctx, err)
		}
	}

	return nil
}

type ExtentMappings struct {
	// input
	InFS          btrfs.Trees
	InLV          *btrfsvol.LogicalVolume[*btrfs.Device]
	InSum2laddrs  map[shortSum][]btrfsvol.LogicalAddr
	InBlockGroups *BlockGroupTree

	// state
	initOnce           sync.Once
	initErr            error
	alg                btrfssum.CSumType
	internedMappingsMu sync.Mutex
	internedMappings   map[btrfsvol.Mapping]*btrfsvol.Mapping

	// output
	sum2lock        map[shortSum]*sync.Mutex
	OutSum2mappings map[shortSum][]*btrfsvol.Mapping
}

func (em *ExtentMappings) init() error {
	em.initOnce.Do(func() {
		sb, err := em.InFS.Superblock()
		if err != nil {
			em.initErr = err
			return
		}
		em.alg = sb.ChecksumType
		em.internedMappings = make(map[btrfsvol.Mapping]*btrfsvol.Mapping)
		em.sum2lock = make(map[shortSum]*sync.Mutex, len(em.InSum2laddrs))
		for sum := range em.InSum2laddrs {
			em.sum2lock[sum] = new(sync.Mutex)
		}
		em.OutSum2mappings = make(map[shortSum][]*btrfsvol.Mapping)
	})
	return em.initErr
}

func (em *ExtentMappings) considerMapping(dev *btrfs.Device, laddr btrfsvol.LogicalAddr, paddr btrfsvol.QualifiedPhysicalAddr) (btrfsvol.Mapping, bool) {
	blockgroup := LookupBlockGroup(em.InBlockGroups, laddr, csumBlockSize)
	if blockgroup == nil {
		return btrfsvol.Mapping{
			LAddr: laddr,
			PAddr: paddr,
			Size:  csumBlockSize,
		}, true
	}
	mapping := btrfsvol.Mapping{
		LAddr: blockgroup.LAddr,
		PAddr: btrfsvol.QualifiedPhysicalAddr{
			Dev:  paddr.Dev,
			Addr: paddr.Addr.Add(laddr.Sub(blockgroup.LAddr)),
		},
		Size:       blockgroup.Size,
		SizeLocked: true,
		Flags: containers.Optional[btrfsvol.BlockGroupFlags]{
			OK:  true,
			Val: blockgroup.Flags,
		},
	}
	if !em.InLV.CouldAddMapping(mapping) {
		return btrfsvol.Mapping{}, false
	}

	for offset := btrfsvol.AddrDelta(0); offset <= mapping.Size; offset += csumBlockSize {
		expCSum, err := LookupCSum(em.InFS, em.alg, mapping.LAddr.Add(offset))
		if err != nil {
			continue
		}
		actCSum, err := ChecksumPhysical(dev, em.alg, mapping.PAddr.Addr.Add(offset))
		if err != nil {
			return btrfsvol.Mapping{}, false
		}
		if actCSum != expCSum {
			return btrfsvol.Mapping{}, false
		}
	}
	return mapping, true
}

func (em *ExtentMappings) addMapping(sum shortSum, mapping btrfsvol.Mapping) {
	em.internedMappingsMu.Lock()
	interned := em.internedMappings[mapping]
	if interned == nil {
		interned = &mapping
		em.internedMappings[mapping] = interned
	}
	em.internedMappingsMu.Unlock()

	em.sum2lock[sum].Lock()
	em.OutSum2mappings[sum] = append(em.OutSum2mappings[sum], interned)
	em.sum2lock[sum].Unlock()
}

func (em *ExtentMappings) ScanOneDev(ctx context.Context, dev *btrfs.Device, gaps []PhysicalGap) error {
	if err := em.init(); err != nil {
		return err
	}
	devID := func() btrfsvol.DeviceID {
		sb, _ := dev.Superblock()
		return sb.DevItem.DevID
	}()

	dlog.Infof(ctx, "... dev[%q] Scanning for extents...", dev.Name())

	sumSize := em.alg.Size()

	lastProgress := -1
	return WalkGapsOneDev(ctx, dev, gaps, csumBlockSize,
		func(curBlock, totalBlocks int64) {
			pct := int(100 * float64(curBlock) / float64(totalBlocks))
			if pct != lastProgress || curBlock == totalBlocks {
				dlog.Infof(ctx, "... dev[%q] scanned %v%%",
					dev.Name(), pct)
				lastProgress = pct
			}
		},
		func(paddr btrfsvol.PhysicalAddr) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			sum, err := ChecksumPhysical(dev, em.alg, paddr)
			if err != nil {
				dlog.Errorf(ctx, "... dev[%s] error: checksumming paddr=%v: %v",
					dev.Name(), paddr, err)
				return nil
			}
			shortSum := shortSum(sum[:sumSize])

			for _, laddr := range em.InSum2laddrs[shortSum] {
				if err := ctx.Err(); err != nil {
					return err
				}
				mapping, ok := em.considerMapping(dev, laddr, btrfsvol.QualifiedPhysicalAddr{
					Dev:  devID,
					Addr: paddr,
				})
				if !ok {
					continue
				}
				em.addMapping(shortSum, mapping)
			}

			return nil
		},
	)
}
