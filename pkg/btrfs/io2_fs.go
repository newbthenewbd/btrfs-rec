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

	uuid2dev         map[UUID]*Device
	logical2physical [][]mapping
	physical2logical map[UUID][]mapping

	cacheSuperblocks []*util.Ref[PhysicalAddr, Superblock]
	cacheSuperblock  *util.Ref[PhysicalAddr, Superblock]
}

func (fs *FS) Name() string {
	sb, err := fs.Superblock()
	if err != nil {
		return fmt.Sprintf("fs_uuid=%v", "(unreadable)")
	}
	return fmt.Sprintf("fs_uuid=%v", sb.Data.FSUUID)
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

// logical => []physical
type chunkMapping struct {
	LAddr  LogicalAddr
	PAddrs map[QualifiedPhysicalAddr]struct{}
	Size   AddrDelta
	Flags  *btrfsitem.BlockGroupFlags
}

// return -1 if 'a' is wholly to the left of 'b'
// return 0 if there is some overlap between 'a' and 'b'
// return 1 if 'a is wholly to the right of 'b'
func (a chunkMapping) cmpRange(b chunkMapping) bool {
	switch {
	case a.LAddr+a.Size <= b.LAddr:
		// 'a' is wholly to the left of 'b'.
		return -1
	case b.LAddr+b.Size <= a.LAddr:
		// 'a' is wholly to the right of 'b'.
		return 1
	default:
		// There is some overlap.
		return 0
	}
}

func (a chunkMapping) union(rest ...chunkMapping) (chunkMapping, error) {
	// sanity check
	for _, chunk := range rest {
		if a.cmpRange(chunk) != 0 {
			return chunkMapping{}, fmt.Errorf("chunks don't overlap")
		}
	}
	chunks := append([]chunkMapping{a}, rest...)
	// figure out the logical range (.LAddr and .Size)
	beg := chunks[0].LAddr
	end := chunks[0].LAddr.Add(chunks[0].Size)
	for _, chunk := range chunks {
		beg = util.Min(beg, chunk.LAddr)
		end = util.Max(end, chunk.LAddr.Add(chunk.Size))
	}
	ret := chunkMapping{
		LAddr: beg,
		Size:  end.Sub(beg),
	}
	// figure out the physical stripes (.PAddrs)
	ret.PAddrs = make(map[QualifiedPhysicalAddr]struct{})
	for _, chunk := range chunks {
		offsetWithinRet := chunk.LAddr.Sub(ret.Laddr)
		for stripe := range chunk.PAddrs {
			ret.PAddrs[QualifiedPhysicalAddr{
				Dev:  stripe.Dev,
				Addr: stripe.Addr.Add(-offsetWithinRet),
			}] = struct{}{}
		}
	}
	// figure out the flags (.Flags)
	for _, chunk := range chunks {
		if chunk.Flags == nil {
			continue
		}
		if ret.Flags == nil {
			val := *chunk.Flags
			ret.Flags = &val
		}
		if *ret.Flags != *chunk.Flags {
			return ret, fmt.Errorf("mismatch flags: %v != %v", *ret.Flags, *chunk.Flags)
		}
	}
	// done
	return ret, nil
}

// physical => logical
type devextMapping struct {
	PAddr QualifiedPhysicalAddr
	LAddr LogicalAddr
	Size  AddrDelta
	Flags *btrfsitem.BlockGroupFlags
}

// return -2 if 'a' and 'b' are on different devices
// return -1 if 'a' is wholly to the left of 'b'
// return 0 if there is some overlap between 'a' and 'b'
// return 1 if 'a is wholly to the right of 'b'
func (a devextMapping) cmpRange(b devextMapping) bool {
	switch {
	case a.PAddr.Dev != b.PAddr.Dev:
		// 'a' and 'b' are on different devices.
		return -2
	case a.PAddr.Addr+a.Size <= b.PAddr.Addr:
		// 'a' is wholly to the left of 'b'.
		return -1
	case b.PAddr.Addr+b.Size <= a.PAddr.Addr:
		// 'a' is wholly to the right of 'b'.
		return 1
	default:
		// There is some overlap.
		return 0
	}
}

func (a devextMapping) union(rest ...devextMapping) (devextMapping, error) {
	// sanity check
	for _, ext := range rest {
		if a.cmpRange(ext) != 0 {
			return devextMapping{}, fmt.Errorf("devexts don't overlap")
		}
	}
	exts := append([]devextMapping{a}, rest...)
	// figure out the physical range (.PAddr and .Size)
	beg := exts[0].PAddr.Addr
	end := beg.Add(exts[0].Size)
	for _, ext := range exts {
		beg = util.Min(beg, ext.PAddr.Addr)
		end = util.Max(end, ext.PAddr.Addr.Add(ext.Size))
	}
	ret := devextMapping{
		PAddr: QualifiedPhysicalAddr{
			Dev:  exts[0].PAddr.Dev,
			Addr: beg,
		},
		Size: end.Sub(beg),
	}
	// figure out the logical range (.LAddr)
	first := true
	for _, ext := range exts {
		offsetWithinRet := ext.PAddr.Addr.Sub(ret.PAddr.Addr)
		laddr := ext.LAddr.Add(-offsetWithinRet)
		if first {
			ret.LAddr = laddr
		} else if laddr != ret.LAddr {
			return ret, fmt.Errorf("devexts don't agree on laddr: %v != %v", ret.LAddr, laddr)
		}
	}
	// figure out the flags (.Flags)
	for _, ext := range exts {
		if ext.Flags == nil {
			continue
		}
		if ret.Flags == nil {
			val := *ext.Flags
			ret.Flags = &val
		}
		if *ret.Flags != *ext.Flags {
			return ret, fmt.Errorf("mismatch flags: %v != %v", *ret.Flags, *ext.Flags)
		}
	}
	// done
	return ret, nil
}

func (fs *FS) AddMapping(laddr LogicalAddr, paddr QualifiedPhysicalAddr, size AddrDelta, flags *btrfsitem.BlockGroupFlags) error {
	// logical2physical
	newChunk := chunkMapping{
		LAddr: laddr,
		PAddr: paddr,
		Size:  size,
		Flags: flags,
	}
	var logicalOverlaps []chunkMapping
	for _, chunk := range fs.logical2physical {
		switch newChunk.cmpRange(chunk) {
		case 0:
			logicalOverlaps = append(logicalOverlaps, chunk)
		case 1:
			break
		}
	}
	if len(logicalOverlaps) > 0 {
		var err error
		newChunk, err = newChunk.union(logicalOverlaps...)
		if err != nil {
			return err
		}
	}

	// physical2logical
	newExt := devextMapping{
		PAddr: paddr,
		LAddr: laddr,
		Size:  size,
		Flags: flags,
	}
	var physicalOverlaps []*mapping
	for _, ext := range fs.physical2logical[m.PAddr.Dev] {
		switch newExt.cmpPhysicalRange(ext) {
		case 0:
			physicalOverlaps = append(physicalOverlaps, ext)
		case 1:
			break
		}
	}
	if len(physicalOverlaps) > 0 {
		var err error
		newExt, err = newExt.union(physicalOverlaps)
		if err != nil {
			return err
		}
	}

	// combine
	if flags == nil {
		if newChunk.Flags != nil {
			if newExt.Flags != nil && *newChunk.Flags != *newExt.Flags {
				return fmt.Errorf("mismatch flags: %v != %v", *newChunk.Flags, *newExt.Flags)
			}
			newExt.Flags = newChunk.Flags
		} else if newExt.Flags != nil {
			newChunk.Flags = newExt.Flags
		}
	}

	// logical2physical
	for _, chunk := range logicalOverlaps {
		fs.logical2physical = util.RemoveFromSlice(fs.logical2physical, chunk)
	}
	fs.logical2physical = append(fs.logical2physical, newChunk)
	sort.Slice(fs.logical2physical, func(i, j int) bool {
		return fs.logical2physical[i].LAddr < fs.logical2physical[j].LAddr
	})

	// physical2logical
	for _, ext := range physicalOverlaps {
		fs.physical2logical[newExt.PAddr.Dev] = util.RemoveFromSlice(fs.physical2logical[newExt.PAddr.Dev], ext)
	}
	fs.physical2logical[newExt.PAddr.Dev] = append(fs.physical2logical[newExt.PAddr.Dev], newExt)
	sort.Slice(fs.physical2logical[newExt.PAddr.Dev], func(i, j int) bool {
		return fs.physical2logical[newExt.PAddr.Dev][i].LAddr < fs.physical2logical[newExt.PAddr.Dev][j].LAddr
	})

	// sanity check

	return nil
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
	fs.uuid2dev = make(map[UUID]*Device, len(fs.Devices))
	fs.chunks = nil
	for _, dev := range fs.Devices {
		sbs, err := dev.Superblocks()
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
		if other, exists := fs.uuid2dev[sb.Data.DevItem.DevUUID]; exists {
			return fmt.Errorf("file %q and file %q have the same device ID: %v",
				other.Name(), dev.Name(), sb.Data.DevItem.DevUUID)
		}
		fs.uuid2dev[sb.Data.DevItem.DevUUID] = dev
		syschunks, err := sb.Data.ParseSysChunkArray()
		if err != nil {
			return fmt.Errorf("file %q: %w", dev.Name(), err)
		}
		for _, chunk := range syschunks {
			fs.chunks = append(fs.chunks, chunk)
		}
		if err := fs.WalkTree(sb.Data.ChunkTree, WalkTreeHandler{
			Item: func(_ WalkTreePath, item Item) error {
				if item.Head.Key.ItemType != btrfsitem.CHUNK_ITEM_KEY {
					return nil
				}
				fs.chunks = append(fs.chunks, SysChunk{
					Key:   item.Head.Key,
					Chunk: item.Body.(btrfsitem.Chunk),
				})
				return nil
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

type QualifiedPhysicalAddr struct {
	Dev  UUID
	Addr PhysicalAddr
}

func (fs *FS) Resolve(laddr LogicalAddr) (paddrs map[QualifiedPhysicalAddr]struct{}, maxlen AddrDelta) {
	paddrs = make(map[QualifiedPhysicalAddr]struct{})
	maxlen = math.MaxInt64

	for _, chunk := range fs.chunks {
		low := LogicalAddr(chunk.Key.Offset)
		high := low.Add(chunk.Chunk.Head.Size)
		if low <= laddr && laddr < high {
			offsetWithinChunk := laddr.Sub(low)
			maxlen = util.Min(maxlen, chunk.Chunk.Head.Size-offsetWithinChunk)
			for _, stripe := range chunk.Chunk.Stripes {
				paddrs[QualifiedPhysicalAddr{
					Dev:  stripe.DeviceUUID,
					Addr: stripe.Offset.Add(offsetWithinChunk),
				}] = struct{}{}
			}
		}
	}

	return paddrs, maxlen
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

func (fs *FS) maybeShortReadAt(dat []byte, laddr LogicalAddr) (int, error) {
	paddrs, maxlen := fs.Resolve(laddr)
	if len(paddrs) == 0 {
		return 0, fmt.Errorf("read: could not map logical address %v", laddr)
	}
	if AddrDelta(len(dat)) > maxlen {
		dat = dat[:maxlen]
	}

	buf := make([]byte, len(dat))
	first := true
	for paddr := range paddrs {
		dev, ok := fs.uuid2dev[paddr.Dev]
		if !ok {
			return 0, fmt.Errorf("device=%v does not exist", paddr.Dev)
		}
		if _, err := dev.ReadAt(buf, paddr.Addr); err != nil {
			return 0, fmt.Errorf("read device=%v paddr=%v: %w", paddr.Dev, paddr.Addr, err)
		}
		if first {
			copy(dat, buf)
		} else {
			if !bytes.Equal(dat, buf) {
				return 0, fmt.Errorf("inconsistent stripes at laddr=%v len=%v", laddr, len(dat))
			}
		}
	}
	return len(dat), nil
}

func (fs *FS) WriteAt(dat []byte, laddr LogicalAddr) (int, error) {
	done := 0
	for done < len(dat) {
		n, err := fs.maybeShortWriteAt(dat[done:], laddr+LogicalAddr(done))
		done += n
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

func (fs *FS) maybeShortWriteAt(dat []byte, laddr LogicalAddr) (int, error) {
	paddrs, maxlen := fs.Resolve(laddr)
	if len(paddrs) == 0 {
		return 0, fmt.Errorf("write: could not map logical address %v", laddr)
	}
	if AddrDelta(len(dat)) > maxlen {
		dat = dat[:maxlen]
	}

	for paddr := range paddrs {
		dev, ok := fs.uuid2dev[paddr.Dev]
		if !ok {
			return 0, fmt.Errorf("device=%v does not exist", paddr.Dev)
		}
		if _, err := dev.WriteAt(dat, paddr.Addr); err != nil {
			return 0, fmt.Errorf("write device=%v paddr=%v: %w", paddr.Dev, paddr.Addr, err)
		}
	}
	return len(dat), nil
}
