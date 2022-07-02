package btrfs

import (
	"fmt"
	"io"

	"github.com/datawire/dlib/derror"

	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type FS struct {
	// You should probably not access .LV directly, except when
	// implementing special things like fsck.
	LV btrfsvol.LogicalVolume[*Device]

	cacheSuperblocks []*util.Ref[PhysicalAddr, Superblock]
	cacheSuperblock  *util.Ref[PhysicalAddr, Superblock]
}

var _ util.File[LogicalAddr] = (*FS)(nil)

func (fs *FS) AddDevice(dev *Device) error {
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
		return err
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

func (fs *FS) Size() (LogicalAddr, error) {
	return fs.LV.Size()
}

func (fs *FS) ReadAt(p []byte, off LogicalAddr) (int, error) {
	return fs.LV.ReadAt(p, off)
}
func (fs *FS) WriteAt(p []byte, off LogicalAddr) (int, error) {
	return fs.LV.WriteAt(p, off)
}

func (fs *FS) Resolve(laddr LogicalAddr) (paddrs map[QualifiedPhysicalAddr]struct{}, maxlen AddrDelta) {
	return fs.LV.Resolve(laddr)
}

func (fs *FS) Superblocks() ([]*util.Ref[PhysicalAddr, Superblock], error) {
	if fs.cacheSuperblocks != nil {
		return fs.cacheSuperblocks, nil
	}
	var ret []*util.Ref[PhysicalAddr, Superblock]
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

func (fs *FS) Superblock() (*util.Ref[PhysicalAddr, Superblock], error) {
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
			// This is probably wrong, but lots of my
			// multi-device code is probably wrong.
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

func (fs *FS) initDev(sb *util.Ref[PhysicalAddr, Superblock]) error {
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
	if err := fs.TreeWalk(sb.Data.ChunkTree, TreeWalkHandler{
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
	}); err != nil {
		return err
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
