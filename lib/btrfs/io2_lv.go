// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
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
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type FS struct {
	// You should probably not access .LV directly, except when
	// implementing special things like fsck.
	LV btrfsvol.LogicalVolume[*Device]

	cacheSuperblocks []*diskio.Ref[btrfsvol.PhysicalAddr, btrfstree.Superblock]
	cacheSuperblock  *btrfstree.Superblock

	cacheObjID2All  map[btrfsprim.ObjID]treeInfo
	cacheUUID2ObjID map[btrfsprim.UUID]btrfsprim.ObjID
}

var _ diskio.File[btrfsvol.LogicalAddr] = (*FS)(nil)

func (fs *FS) AddDevice(ctx context.Context, dev *Device) error {
	sb, err := dev.Superblock()
	if err != nil {
		return err
	}
	if err := fs.LV.AddPhysicalVolume(sb.DevItem.DevID, dev); err != nil {
		return err
	}
	fs.cacheSuperblocks = nil
	fs.cacheSuperblock = nil
	if err := fs.initDev(ctx, *sb); err != nil {
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
	name := fmt.Sprintf("fs_uuid=%v", sb.FSUUID)
	fs.LV.SetName(name)
	return name
}

func (fs *FS) Size() btrfsvol.LogicalAddr {
	return fs.LV.Size()
}

func (fs *FS) ReadAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return fs.LV.ReadAt(p, off)
}

func (fs *FS) WriteAt(p []byte, off btrfsvol.LogicalAddr) (int, error) {
	return fs.LV.WriteAt(p, off)
}

func (fs *FS) Superblocks() ([]*diskio.Ref[btrfsvol.PhysicalAddr, btrfstree.Superblock], error) {
	if fs.cacheSuperblocks != nil {
		return fs.cacheSuperblocks, nil
	}
	var ret []*diskio.Ref[btrfsvol.PhysicalAddr, btrfstree.Superblock]
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

func (fs *FS) Superblock() (*btrfstree.Superblock, error) {
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

	fs.cacheSuperblock = &sbs[0].Data
	return &sbs[0].Data, nil
}

func (fs *FS) ReInit(ctx context.Context) error {
	fs.LV.ClearMappings()
	for _, dev := range fs.LV.PhysicalVolumes() {
		sb, err := dev.Superblock()
		if err != nil {
			return fmt.Errorf("file %q: %w", dev.Name(), err)
		}
		if err := fs.initDev(ctx, *sb); err != nil {
			return fmt.Errorf("file %q: %w", dev.Name(), err)
		}
	}
	return nil
}

func (fs *FS) initDev(ctx context.Context, sb btrfstree.Superblock) error {
	syschunks, err := sb.ParseSysChunkArray()
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
	fs.TreeWalk(ctx, btrfsprim.CHUNK_TREE_OBJECTID,
		func(err *btrfstree.TreeError) {
			errs = append(errs, err)
		},
		btrfstree.TreeWalkHandler{
			Item: func(_ btrfstree.Path, item btrfstree.Item) {
				if item.Key.ItemType != btrfsitem.CHUNK_ITEM_KEY {
					return
				}
				switch itemBody := item.Body.(type) {
				case *btrfsitem.Chunk:
					for _, mapping := range itemBody.Mappings(item.Key) {
						if err := fs.LV.AddMapping(mapping); err != nil {
							errs = append(errs, err)
						}
					}
				case *btrfsitem.Error:
					// do nothing
				default:
					// This is a panic because the item decoder should not emit CHUNK_ITEM items as
					// anything but btrfsitem.Chunk or btrfsitem.Error without this code also being
					// updated.
					panic(fmt.Errorf("should not happen: CHUNK_ITEM has unexpected item type: %T", itemBody))
				}
			},
		},
	)
	if len(errs) > 0 {
		return errs
	}
	return nil
}

func (fs *FS) Close() error {
	return fs.LV.Close()
}

var _ io.Closer = (*FS)(nil)
