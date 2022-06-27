package btrfs

import (
	"fmt"
	"reflect"

	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type FS struct {
	lv btrfsvol.LogicalVolume

	cacheSuperblocks []*util.Ref[PhysicalAddr, Superblock]
	cacheSuperblock  *util.Ref[PhysicalAddr, Superblock]
}

var _ util.File[LogicalAddr] = (*FS)(nil)

func (fs *FS) AddDevice(dev *Device) error {
	sb, err := dev.Superblock()
	if err != nil {
		return err
	}
	return fs.lv.AddPhysicalVolume(sb.Data.DevItem.DevUUID, dev)
}

func (fs *FS) Name() string {
	if name := fs.lv.Name(); name != "" {
		return name
	}
	sb, err := fs.Superblock()
	if err != nil {
		return fmt.Sprintf("fs_uuid=%v", "(unreadable)")
	}
	name := fmt.Sprintf("fs_uuid=%v", sb.Data.FSUUID)
	fs.lv.SetName(name)
	return name
}

func (fs *FS) Size() (LogicalAddr, error) {
	return fs.lv.Size()
}

func (fs *FS) ReadAt(p []byte, off LogicalAddr) (int, error) {
	return fs.lv.ReadAt(p, off)
}
func (fs *FS) WriteAt(p []byte, off LogicalAddr) (int, error) {
	return fs.lv.WriteAt(p, off)
}

func (fs *FS) Resolve(laddr LogicalAddr) (paddrs map[QualifiedPhysicalAddr]struct{}, maxlen AddrDelta) {
	return fs.lv.Resolve(laddr)
}

func (fs *FS) UnResolve(paddr QualifiedPhysicalAddr) LogicalAddr {
	return fs.lv.UnResolve(paddr)
}

func (fs *FS) Devices() []*Device {
	untyped := fs.lv.PhysicalVolumes()
	typed := make([]*Device, len(untyped))
	for i := range untyped {
		typed[i] = untyped[i].(*Device)
	}
	return typed
}

func (fs *FS) Superblocks() ([]*util.Ref[PhysicalAddr, Superblock], error) {
	if fs.cacheSuperblocks != nil {
		return fs.cacheSuperblocks, nil
	}
	var ret []*util.Ref[PhysicalAddr, Superblock]
	for _, dev := range fs.lv.PhysicalVolumes() {
		sbs, err := dev.(*Device).Superblocks()
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

func (fs *FS) Init() error {
	fs.lv.ClearMappings()
	for _, dev := range fs.lv.PhysicalVolumes() {
		sbs, err := dev.(*Device).Superblocks()
		if err != nil {
			return fmt.Errorf("file %q: %w", dev.Name(), err)
		}

		a := sbs[0].Data
		a.Checksum = CSum{}
		a.Self = 0
		for i, sb := range sbs[1:] {
			b := sb.Data
			b.Checksum = CSum{}
			b.Self = 0
			if !reflect.DeepEqual(a, b) {
				return fmt.Errorf("file %q: superblock %v disagrees with superblock 0",
					dev.Name(), i+1)
			}
		}
		sb := sbs[0]
		syschunks, err := sb.Data.ParseSysChunkArray()
		if err != nil {
			return fmt.Errorf("file %q: %w", dev.Name(), err)
		}
		for _, chunk := range syschunks {
			for _, stripe := range chunk.Chunk.Stripes {
				if err := fs.lv.AddMapping(
					LogicalAddr(chunk.Key.Offset),
					QualifiedPhysicalAddr{
						Dev:  stripe.DeviceUUID,
						Addr: stripe.Offset,
					},
					chunk.Chunk.Head.Size,
					&chunk.Chunk.Head.Type,
				); err != nil {
					return fmt.Errorf("file %q: %w", dev.Name(), err)
				}
			}
		}
		if err := fs.WalkTree(sb.Data.ChunkTree, WalkTreeHandler{
			Item: func(_ WalkTreePath, item Item) error {
				if item.Head.Key.ItemType != btrfsitem.CHUNK_ITEM_KEY {
					return nil
				}
				body := item.Body.(btrfsitem.Chunk)
				for _, stripe := range body.Stripes {
					if err := fs.lv.AddMapping(
						LogicalAddr(item.Head.Key.Offset),
						QualifiedPhysicalAddr{
							Dev:  stripe.DeviceUUID,
							Addr: stripe.Offset,
						},
						body.Head.Size,
						&body.Head.Type,
					); err != nil {
						return fmt.Errorf("file %q: %w", dev.Name(), err)
					}
				}
				return nil
			},
		}); err != nil {
			return err
		}
	}
	return nil
}
