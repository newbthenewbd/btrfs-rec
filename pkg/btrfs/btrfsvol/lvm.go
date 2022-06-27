package btrfsvol

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"sort"

	"lukeshu.com/btrfs-tools/pkg/util"
)

type LogicalVolume[PhysicalVolume util.File[PhysicalAddr]] struct {
	name string

	uuid2pv map[util.UUID]PhysicalVolume

	logical2physical []chunkMapping
	physical2logical map[util.UUID][]devextMapping
}

var _ util.File[LogicalAddr] = (*LogicalVolume[util.File[PhysicalAddr]])(nil)

func (lv *LogicalVolume[PhysicalVolume]) SetName(name string) {
	lv.name = name
}

func (lv *LogicalVolume[PhysicalVolume]) Name() string {
	return lv.name
}

func (lv *LogicalVolume[PhysicalVolume]) Size() (LogicalAddr, error) {
	if len(lv.logical2physical) == 0 {
		return 0, nil
	}
	lastChunk := lv.logical2physical[len(lv.logical2physical)-1]
	return lastChunk.LAddr.Add(lastChunk.Size), nil
}

func (lv *LogicalVolume[PhysicalVolume]) AddPhysicalVolume(uuid util.UUID, dev PhysicalVolume) error {
	if lv.uuid2pv == nil {
		lv.uuid2pv = make(map[util.UUID]PhysicalVolume)
	}
	if other, exists := lv.uuid2pv[uuid]; exists {
		return fmt.Errorf("(%p).AddPhysicalVolume: cannot add physical volume %q: already have physical volume %q with uuid=%v",
			lv, dev.Name(), other.Name(), uuid)
	}
	lv.uuid2pv[uuid] = dev
	return nil
}

func (lv *LogicalVolume[PhysicalVolume]) PhysicalVolumes() []PhysicalVolume {
	uuids := make([]util.UUID, 0, len(lv.uuid2pv))
	for uuid := range lv.uuid2pv {
		uuids = append(uuids, uuid)
	}
	sort.Slice(uuids, func(i, j int) bool {
		return uuids[i].Cmp(uuids[j]) < 0
	})
	ret := make([]PhysicalVolume, 0, len(lv.uuid2pv))
	for _, uuid := range uuids {
		ret = append(ret, lv.uuid2pv[uuid])
	}
	return ret
}

func (lv *LogicalVolume[PhysicalVolume]) ClearMappings() {
	lv.logical2physical = nil
	lv.physical2logical = nil
}

func (lv *LogicalVolume[PhysicalVolume]) AddMapping(laddr LogicalAddr, paddr QualifiedPhysicalAddr, size AddrDelta, flags *BlockGroupFlags) error {
	// sanity check
	if _, haveDev := lv.uuid2pv[paddr.Dev]; !haveDev {
		return fmt.Errorf("(%p).AddMapping: do not have a physical volume with uuid=%v",
			lv, paddr.Dev)
	}
	if lv.physical2logical == nil {
		lv.physical2logical = make(map[util.UUID][]devextMapping)
	}

	// logical2physical
	newChunk := chunkMapping{
		LAddr:  laddr,
		PAddrs: []QualifiedPhysicalAddr{paddr},
		Size:   size,
		Flags:  flags,
	}
	var logicalOverlaps []chunkMapping
	for _, chunk := range lv.logical2physical {
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
			return fmt.Errorf("(%p).AddMapping: %w", lv, err)
		}
	}

	// physical2logical
	newExt := devextMapping{
		PAddr: paddr.Addr,
		LAddr: laddr,
		Size:  size,
		Flags: flags,
	}
	var physicalOverlaps []devextMapping
	for _, ext := range lv.physical2logical[paddr.Dev] {
		switch newExt.cmpRange(ext) {
		case 0:
			physicalOverlaps = append(physicalOverlaps, ext)
		case 1:
			break
		}
	}
	if len(physicalOverlaps) > 0 {
		var err error
		newExt, err = newExt.union(physicalOverlaps...)
		if err != nil {
			return fmt.Errorf("(%p).AddMapping: %w", lv, err)
		}
	}

	// logical2physical
	for _, chunk := range logicalOverlaps {
		lv.logical2physical = util.RemoveAllFromSliceFunc(lv.logical2physical, func(otherChunk chunkMapping) bool {
			return otherChunk.LAddr == chunk.LAddr
		})
	}
	lv.logical2physical = append(lv.logical2physical, newChunk)
	sort.Slice(lv.logical2physical, func(i, j int) bool {
		return lv.logical2physical[i].LAddr < lv.logical2physical[j].LAddr
	})

	// physical2logical
	for _, ext := range physicalOverlaps {
		lv.physical2logical[paddr.Dev] = util.RemoveAllFromSlice(lv.physical2logical[paddr.Dev], ext)
	}
	lv.physical2logical[paddr.Dev] = append(lv.physical2logical[paddr.Dev], newExt)
	sort.Slice(lv.physical2logical[paddr.Dev], func(i, j int) bool {
		return lv.physical2logical[paddr.Dev][i].PAddr < lv.physical2logical[paddr.Dev][j].PAddr
	})

	// sanity check
	//
	// This is in-theory unnescessary, but that assumes that I
	// made no mistakes in my algorithm above.
	if err := lv.fsck(); err != nil {
		return err
	}

	// done
	return nil
}

func (lv *LogicalVolume[PhysicalVolume]) fsck() error {
	physical2logical := make(map[util.UUID][]devextMapping)
	for _, chunk := range lv.logical2physical {
		for _, stripe := range chunk.PAddrs {
			if _, devOK := lv.uuid2pv[stripe.Dev]; !devOK {
				return fmt.Errorf("(%p).fsck: chunk references physical volume %v which does not exist",
					lv, stripe.Dev)
			}
			physical2logical[stripe.Dev] = append(physical2logical[stripe.Dev], devextMapping{
				PAddr: stripe.Addr,
				LAddr: chunk.LAddr,
				Size:  chunk.Size,
				Flags: chunk.Flags,
			})
		}
	}
	for _, exts := range physical2logical {
		sort.Slice(exts, func(i, j int) bool {
			return exts[i].PAddr < exts[j].PAddr
		})
	}

	if !reflect.DeepEqual(lv.physical2logical, physical2logical) {
		return fmt.Errorf("(%p).fsck: skew between chunk tree and devext tree",
			lv)
	}

	return nil
}

func (lv *LogicalVolume[PhysicalVolume]) Resolve(laddr LogicalAddr) (paddrs map[QualifiedPhysicalAddr]struct{}, maxlen AddrDelta) {
	paddrs = make(map[QualifiedPhysicalAddr]struct{})
	maxlen = math.MaxInt64

	for _, chunk := range lv.logical2physical {
		low := chunk.LAddr
		high := low.Add(chunk.Size)
		if low <= laddr && laddr < high {
			offsetWithinChunk := laddr.Sub(low)
			maxlen = util.Min(maxlen, chunk.Size-offsetWithinChunk)
			for _, stripe := range chunk.PAddrs {
				paddrs[QualifiedPhysicalAddr{
					Dev:  stripe.Dev,
					Addr: stripe.Addr.Add(offsetWithinChunk),
				}] = struct{}{}
			}
		}
	}

	return paddrs, maxlen
}

func (lv *LogicalVolume[PhysicalVolume]) UnResolve(paddr QualifiedPhysicalAddr) LogicalAddr {
	for _, ext := range lv.physical2logical[paddr.Dev] {
		low := ext.PAddr
		high := low.Add(ext.Size)
		if low <= paddr.Addr && paddr.Addr < high {
			offsetWithinExt := paddr.Addr.Sub(low)
			return ext.LAddr.Add(offsetWithinExt)
		}
	}
	return -1
}

func (lv *LogicalVolume[PhysicalVolume]) ReadAt(dat []byte, laddr LogicalAddr) (int, error) {
	done := 0
	for done < len(dat) {
		n, err := lv.maybeShortReadAt(dat[done:], laddr+LogicalAddr(done))
		done += n
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

func (lv *LogicalVolume[PhysicalVolume]) maybeShortReadAt(dat []byte, laddr LogicalAddr) (int, error) {
	paddrs, maxlen := lv.Resolve(laddr)
	if len(paddrs) == 0 {
		return 0, fmt.Errorf("read: could not map logical address %v", laddr)
	}
	if AddrDelta(len(dat)) > maxlen {
		dat = dat[:maxlen]
	}

	buf := make([]byte, len(dat))
	first := true
	for paddr := range paddrs {
		dev, ok := lv.uuid2pv[paddr.Dev]
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

func (lv *LogicalVolume[PhysicalVolume]) WriteAt(dat []byte, laddr LogicalAddr) (int, error) {
	done := 0
	for done < len(dat) {
		n, err := lv.maybeShortWriteAt(dat[done:], laddr+LogicalAddr(done))
		done += n
		if err != nil {
			return done, err
		}
	}
	return done, nil
}

func (lv *LogicalVolume[PhysicalVolume]) maybeShortWriteAt(dat []byte, laddr LogicalAddr) (int, error) {
	paddrs, maxlen := lv.Resolve(laddr)
	if len(paddrs) == 0 {
		return 0, fmt.Errorf("write: could not map logical address %v", laddr)
	}
	if AddrDelta(len(dat)) > maxlen {
		dat = dat[:maxlen]
	}

	for paddr := range paddrs {
		dev, ok := lv.uuid2pv[paddr.Dev]
		if !ok {
			return 0, fmt.Errorf("device=%v does not exist", paddr.Dev)
		}
		if _, err := dev.WriteAt(dat, paddr.Addr); err != nil {
			return 0, fmt.Errorf("write device=%v paddr=%v: %w", paddr.Dev, paddr.Addr, err)
		}
	}
	return len(dat), nil
}
