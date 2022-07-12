// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"context"
	"fmt"
	"io"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

type FS struct {
	// You should probably not access .LV directly, except when
	// implementing special things like fsck.
	LV btrfsvol.LogicalVolume[*Device]

	cacheSuperblocks []*util.Ref[btrfsvol.PhysicalAddr, Superblock]
	cacheSuperblock  *util.Ref[btrfsvol.PhysicalAddr, Superblock]
}

var _ util.File[btrfsvol.LogicalAddr] = (*FS)(nil)

func (fs *FS) AddDevice(ctx context.Context, dev *Device) error {
	sb, err := dev.Superblock()
	if err != nil {
		return err
	}
	if err := fs.LV.AddPhysicalVolume(sb.Data.DevItem.DevID, dev); err != nil {
		return err
	}
	fs.cacheSuperblocks = nil
	fs.cacheSuperblock = nil
	if err := fs.initDev(sb); err != nil {
		dlog.Errorf(ctx, "error: AddDevice: %q: %v", dev.Name(), err)
	}
	return nil
}

func (fs *FS) Name() string {
	if name := fs.LV.Name(); name != "" {
		return name
	}
	sb, err := fs.Superblock()
	if err != nil {
		return fmt.Sprintf("fs_uuid=%v", "(unreadable)")
	}
	name := fmt.Sprintf("fs_uuid=%v", sb.Data.FSUUID)
	fs.LV.SetName(name)
	return name
}

func (fs *FS) Size() (btrfsvol.LogicalAddr, error) {
	return fs.LV.Size()
}

func (fs *FS) ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return fs.LV.ReadAt(p, off)
}
func (fs *FS) WriteAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return fs.LV.WriteAt(p, off)
}

func (fs *FS) Resolve(laddr btrfsvol.LogicalAddr) (paddrs map[btrfsvol.QualifiedPhysicalAddr]struct{}, maxlen btrfsvol.AddrDelta) {
	return fs.LV.Resolve(laddr)
}

func (fs *FS) Superblocks() ([]*util.Ref[btrfsvol.PhysicalAddr, Superblock], error) {
	if fs.cacheSuperblocks != nil {
		return fs.cacheSuperblocks, nil
	}
	var ret []*util.Ref[btrfsvol.PhysicalAddr, Superblock]
	devs := fs.LV.PhysicalVolumes()
	if len(devs) == 0 {
		return nil, fmt.Errorf("no devices")
	}
	for _, dev := range devs {
		sbs, err := dev.Superblocks()
		if err != nil {
			return nil, fmt.Errorf("file %q: %w", dev.Name(), err)
		}
		ret = append(ret, sbs...)
	}
	fs.cacheSuperblocks = ret
	return ret, nil
}

func (fs *FS) Superblock() (*util.Ref[btrfsvol.PhysicalAddr, Superblock], error) {
	if fs.cacheSuperblock != nil {
		return fs.cacheSuperblock, nil
	}
	sbs, err := fs.Superblocks()
	if err != nil {
		return nil, err
	}
	if len(sbs) == 0 {
		return nil, fmt.Errorf("no superblocks")
	}

	fname := ""
	sbi := 0
	for i, sb := range sbs {
		if sb.File.Name() != fname {
			fname = sb.File.Name()
			sbi = 0
		} else {
			sbi++
		}

		if err := sb.Data.ValidateChecksum(); err != nil {
			return nil, fmt.Errorf("file %q superblock %v: %w", sb.File.Name(), sbi, err)
		}
		if i > 0 {
			// FIXME(lukeshu): This is probably wrong, but
			// lots of my multi-device code is probably
			// wrong.
			if !sb.Data.Equal(sbs[0].Data) {
				return nil, fmt.Errorf("file %q superblock %v and file %q superblock %v disagree",
					sbs[0].File.Name(), 0,
					sb.File.Name(), sbi)
			}
		}
	}

	fs.cacheSuperblock = sbs[0]
	return sbs[0], nil
}

func (fs *FS) ReInit() error {
	fs.LV.ClearMappings()
	for _, dev := range fs.LV.PhysicalVolumes() {
		sb, err := dev.Superblock()
		if err != nil {
			return fmt.Errorf("file %q: %w", dev.Name(), err)
		}
		if err := fs.initDev(sb); err != nil {
			return fmt.Errorf("file %q: %w", dev.Name(), err)
		}
	}
	return nil
}

func (fs *FS) initDev(sb *util.Ref[btrfsvol.PhysicalAddr, Superblock]) error {
	syschunks, err := sb.Data.ParseSysChunkArray()
	if err != nil {
		return err
	}
	for _, chunk := range syschunks {
		for _, mapping := range chunk.Chunk.Mappings(chunk.Key) {
			if err := fs.LV.AddMapping(mapping); err != nil {
				return err
			}
		}
	}
	var errs derror.MultiError
	fs.TreeWalk(CHUNK_TREE_OBJECTID,
		func(err *TreeError) {
			errs = append(errs, err)
		},
		TreeWalkHandler{
			Item: func(_ TreePath, item Item) error {
				if item.Head.Key.ItemType != btrfsitem.CHUNK_ITEM_KEY {
					return nil
				}
				for _, mapping := range item.Body.(btrfsitem.Chunk).Mappings(item.Head.Key) {
					if err := fs.LV.AddMapping(mapping); err != nil {
						return err
					}
				}
				return nil
			},
		},
	)
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (fs *FS) Close() error {
	var errs derror.MultiError
	for _, dev := range fs.LV.PhysicalVolumes() {
		if err := dev.Close(); err != nil && err == nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return errs
	}
	return nil
}

var _ io.Closer = (*FS)(nil)
