package btrfs

import (
	"bytes"
	"fmt"
	"math"
	"reflect"

	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type FS struct {
	Devices []*Device

	initErr  error
	uuid2dev map[UUID]*Device
	chunks   []SysChunk

	cacheSuperblocks []*util.Ref[PhysicalAddr, Superblock]
	cacheSuperblock  *util.Ref[PhysicalAddr, Superblock]
}

func (fs *FS) Name() string {
	sb, err := fs.Superblock()
	if err != nil {
		return fmt.Sprintf("fs_uuid=%s", "(unreadable)")
	}
	return fmt.Sprintf("fs_uuid=%s", sb.Data.FSUUID)
}

func (fs *FS) Size() (LogicalAddr, error) {
	var ret LogicalAddr
	for _, dev := range fs.Devices {
		sz, err := dev.Size()
		if err != nil {
			return 0, fmt.Errorf("file %q: %w", dev.Name(), err)
		}
		ret += LogicalAddr(sz)
	}
	return ret, nil
}

func (fs *FS) Superblocks() ([]*util.Ref[PhysicalAddr, Superblock], error) {
	if fs.cacheSuperblocks != nil {
		return fs.cacheSuperblocks, nil
	}
	var ret []*util.Ref[PhysicalAddr, Superblock]
	for _, dev := range fs.Devices {
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
			return nil, fmt.Errorf("file %q superblock %d: %w", sb.File.Name(), sbi, err)
		}
		if i > 0 {
			// This is probably wrong, but lots of my
			// multi-device code is probably wrong.
			if !sb.Data.Equal(sbs[0].Data) {
				return nil, fmt.Errorf("file %q superblock %d and file %q superblock %d disagree",
					sbs[0].File.Name(), 0,
					sb.File.Name(), sbi)
			}
		}
	}

	fs.cacheSuperblock = sbs[0]
	return sbs[0], nil
}

func (fs *FS) Init() error {
	if fs.uuid2dev != nil {
		return fs.initErr
	}
	fs.uuid2dev = make(map[UUID]*Device, len(fs.Devices))
	for _, dev := range fs.Devices {
		sbs, err := dev.Superblocks()
		if err != nil {
			fs.initErr = fmt.Errorf("file %q: %w", dev.Name(), err)
			return fs.initErr
		}

		a := sbs[0].Data
		a.Checksum = CSum{}
		a.Self = 0
		for i, sb := range sbs[1:] {
			b := sb.Data
			b.Checksum = CSum{}
			b.Self = 0
			if !reflect.DeepEqual(a, b) {
				fs.initErr = fmt.Errorf("file %q: superblock %d disagrees with superblock 0",
					dev.Name(), i+1)
				return fs.initErr
			}
		}
		sb := sbs[0]
		if other, exists := fs.uuid2dev[sb.Data.DevItem.DevUUID]; exists {
			fs.initErr = fmt.Errorf("file %q and file %q have the same device ID: %v",
				other.Name(), dev.Name(), sb.Data.DevItem.DevUUID)
			return fs.initErr
		}
		fs.uuid2dev[sb.Data.DevItem.DevUUID] = dev
		syschunks, err := sb.Data.ParseSysChunkArray()
		if err != nil {
			fs.initErr = fmt.Errorf("file %q: %w", dev.Name(), err)
			return fs.initErr
		}
		for _, chunk := range syschunks {
			fs.chunks = append(fs.chunks, chunk)
		}
		if err := fs.WalkTree(sb.Data.ChunkTree, WalkTreeHandler{
			Item: func(key Key, body btrfsitem.Item) error {
				if key.ItemType != btrfsitem.CHUNK_ITEM_KEY {
					return nil
				}
				fs.chunks = append(fs.chunks, SysChunk{
					Key:   key,
					Chunk: body.(btrfsitem.Chunk),
				})
				return nil
			},
		}); err != nil {
			fs.initErr = err
			return fs.initErr
		}
	}
	return nil
}

func (fs *FS) ReadAt(dat []byte, laddr LogicalAddr) (int, error) {
	done := 0
	for done < len(dat) {
		n, err := fs.maybeShortReadAt(dat[done:], laddr+LogicalAddr(done))
		done += n
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

type QualifiedPhysicalAddr struct {
	Dev  UUID
	Addr PhysicalAddr
}

func (fs *FS) Resolve(laddr LogicalAddr) (paddrs map[QualifiedPhysicalAddr]struct{}, maxlen uint64) {
	paddrs = make(map[QualifiedPhysicalAddr]struct{})
	maxlen = math.MaxUint64

	for _, chunk := range fs.chunks {
		if chunk.Key.Offset <= uint64(laddr) && uint64(laddr) < chunk.Key.Offset+uint64(chunk.Chunk.Head.Size) {
			offsetWithinChunk := uint64(laddr) - chunk.Key.Offset
			maxlen = util.Min(maxlen, chunk.Chunk.Head.Size-offsetWithinChunk)
			for _, stripe := range chunk.Chunk.Stripes {
				paddrs[QualifiedPhysicalAddr{
					Dev:  stripe.DeviceUUID,
					Addr: stripe.Offset + PhysicalAddr(offsetWithinChunk),
				}] = struct{}{}
			}
		}
	}

	return paddrs, maxlen
}

func (fs *FS) maybeShortReadAt(dat []byte, laddr LogicalAddr) (int, error) {
	paddrs, maxlen := fs.Resolve(laddr)
	if len(paddrs) == 0 {
		return 0, fmt.Errorf("could not map logical address %v", laddr)
	}
	if uint64(len(dat)) > maxlen {
		dat = dat[:maxlen]
	}

	buf := make([]byte, len(dat))
	first := true
	for paddr := range paddrs {
		dev, ok := fs.uuid2dev[paddr.Dev]
		if !ok {
			return 0, fmt.Errorf("device=%s does not exist", paddr.Dev)
		}
		if _, err := dev.ReadAt(buf, paddr.Addr); err != nil {
			return 0, fmt.Errorf("read device=%s paddr=%v: %w", paddr.Dev, paddr.Addr, err)
		}
		if first {
			copy(dat, buf)
		} else {
			if !bytes.Equal(dat, buf) {
				return 0, fmt.Errorf("inconsistent stripes at laddr=%v len=%d", laddr, len(dat))
			}
		}
	}
	return len(dat), nil
}
