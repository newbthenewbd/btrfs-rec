// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

type phonyFile struct {
	size btrfsvol.PhysicalAddr
	sb   btrfstree.Superblock
}

var _ diskio.File[btrfsvol.PhysicalAddr] = (*phonyFile)(nil)

func NewPhonyFile(size btrfsvol.PhysicalAddr, sb btrfstree.Superblock) *phonyFile {
	return &phonyFile{
		size: size,
		sb:   sb,
	}
}

func (f *phonyFile) Name() string                { return fmt.Sprintf("phony_file:device_id=%v", f.sb.DevItem.DevID) }
func (f *phonyFile) Size() btrfsvol.PhysicalAddr { return f.size }
func (*phonyFile) Close() error                  { return nil }

func (f *phonyFile) ReadAt(p []byte, off btrfsvol.PhysicalAddr) (int, error) {
	if len(p) == int(btrfs.SuperblockSize) && slices.Contains(off, btrfs.SuperblockAddrs) {
		bs, err := binstruct.Marshal(f.sb)
		if err != nil {
			return 0, err
		}
		return copy(p, bs), nil
	}
	panic(fmt.Errorf("%T: should not happen: ReadAt should not be called for a phony file", f))
}

func (f *phonyFile) WriteAt([]byte, btrfsvol.PhysicalAddr) (int, error) {
	panic(fmt.Errorf("%T: should not happen: WriteAt should not be called for a phony file", f))
}
