package btrfs

import (
	"bytes"
	"fmt"
	"reflect"
)

type FS struct {
	Devices []*Device

	initErr  error
	uuid2dev map[UUID]*Device
	chunks   []SysChunk
}

func (fs *FS) init() error {
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
	}
	return nil
}

func (fs *FS) ReadLogicalFull(laddr LogicalAddr, dat []byte) error {
	done := LogicalAddr(0)
	for done < LogicalAddr(len(dat)) {
		n, err := fs.readLogicalMaybeShort(laddr+done, dat[done:])
		if err != nil {
			return err
		}
		done += LogicalAddr(n)
	}
	return nil
}

func (fs *FS) readLogicalMaybeShort(laddr LogicalAddr, dat []byte) (int, error) {
	if err := fs.init(); err != nil {
		return 0, err
	}

	type physicalAddr struct {
		Dev  UUID
		Addr PhysicalAddr
	}

	paddrs := make(map[physicalAddr]struct{})

	for _, chunk := range fs.chunks {
		if chunk.Offset <= uint64(laddr) && uint64(laddr) < chunk.Offset+uint64(chunk.Chunk.Size) {
			offsetWithinChunk := uint64(laddr) - chunk.Offset
			if offsetWithinChunk+uint64(len(dat)) > chunk.Chunk.Size {
				dat = dat[:chunk.Chunk.Size-offsetWithinChunk]
			}
			for _, stripe := range chunk.Chunk.Stripes {
				paddrs[physicalAddr{
					Dev:  stripe.DeviceUUID,
					Addr: PhysicalAddr(stripe.Offset + offsetWithinChunk),
				}] = struct{}{}
			}
		}
	}

	if len(paddrs) == 0 {
		return 0, fmt.Errorf("could not map logical address %v", laddr)
	}

	buf := make([]byte, len(dat))
	first := true
	for paddr := range paddrs {
		dev, ok := fs.uuid2dev[paddr.Dev]
		if !ok {
			return 0, fmt.Errorf("device=%s does not exist", paddr.Dev)
		}
		if _, err := dev.ReadAt(buf, int64(paddr.Addr)); err != nil {
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
