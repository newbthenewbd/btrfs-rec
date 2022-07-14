// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsvol

import (
	"bytes"
	"fmt"
	"os"
	"reflect"

	"github.com/datawire/dlib/derror"

	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type LogicalVolume[PhysicalVolume diskio.File[PhysicalAddr]] struct {
	name string

	id2pv map[DeviceID]PhysicalVolume

	logical2physical *containers.RBTree[containers.NativeOrdered[LogicalAddr], chunkMapping]
	physical2logical map[DeviceID]*containers.RBTree[containers.NativeOrdered[PhysicalAddr], devextMapping]
}

var _ diskio.File[LogicalAddr] = (*LogicalVolume[diskio.File[PhysicalAddr]])(nil)

func (lv *LogicalVolume[PhysicalVolume]) init() {
	if lv.id2pv == nil {
		lv.id2pv = make(map[DeviceID]PhysicalVolume)
	}
	if lv.logical2physical == nil {
		lv.logical2physical = &containers.RBTree[containers.NativeOrdered[LogicalAddr], chunkMapping]{
			KeyFn: func(chunk chunkMapping) containers.NativeOrdered[LogicalAddr] {
				return containers.NativeOrdered[LogicalAddr]{Val: chunk.LAddr}
			},
		}
	}
	if lv.physical2logical == nil {
		lv.physical2logical = make(map[DeviceID]*containers.RBTree[containers.NativeOrdered[PhysicalAddr], devextMapping], len(lv.id2pv))
	}
	for devid := range lv.id2pv {
		if _, ok := lv.physical2logical[devid]; !ok {
			lv.physical2logical[devid] = &containers.RBTree[containers.NativeOrdered[PhysicalAddr], devextMapping]{
				KeyFn: func(ext devextMapping) containers.NativeOrdered[PhysicalAddr] {
					return containers.NativeOrdered[PhysicalAddr]{Val: ext.PAddr}
				},
			}
		}
	}
}

func (lv *LogicalVolume[PhysicalVolume]) SetName(name string) {
	lv.name = name
}

func (lv *LogicalVolume[PhysicalVolume]) Name() string {
	return lv.name
}

func (lv *LogicalVolume[PhysicalVolume]) Size() LogicalAddr {
	lv.init()
	lastChunk := lv.logical2physical.Max()
	if lastChunk == nil {
		return 0
	}
	return lastChunk.Value.LAddr.Add(lastChunk.Value.Size)
}

func (lv *LogicalVolume[PhysicalVolume]) Close() error {
	var errs derror.MultiError
	for _, dev := range lv.id2pv {
		if err := dev.Close(); err != nil && err == nil {
			errs = append(errs, err)
		}
	}
	if errs != nil {
		return errs
	}
	return nil
}
func (lv *LogicalVolume[PhysicalVolume]) AddPhysicalVolume(id DeviceID, dev PhysicalVolume) error {
	lv.init()
	if other, exists := lv.id2pv[id]; exists {
		return fmt.Errorf("(%p).AddPhysicalVolume: cannot add physical volume %q: already have physical volume %q with id=%v",
			lv, dev.Name(), other.Name(), id)
	}
	lv.id2pv[id] = dev
	lv.physical2logical[id] = &containers.RBTree[containers.NativeOrdered[PhysicalAddr], devextMapping]{
		KeyFn: func(ext devextMapping) containers.NativeOrdered[PhysicalAddr] {
			return containers.NativeOrdered[PhysicalAddr]{Val: ext.PAddr}
		},
	}
	return nil
}

func (lv *LogicalVolume[PhysicalVolume]) PhysicalVolumes() map[DeviceID]PhysicalVolume {
	dup := make(map[DeviceID]PhysicalVolume, len(lv.id2pv))
	for k, v := range lv.id2pv {
		dup[k] = v
	}
	return dup
}

func (lv *LogicalVolume[PhysicalVolume]) ClearMappings() {
	lv.logical2physical = nil
	lv.physical2logical = nil
}

type Mapping struct {
	LAddr      LogicalAddr
	PAddr      QualifiedPhysicalAddr
	Size       AddrDelta
	SizeLocked bool             `json:",omitempty"`
	Flags      *BlockGroupFlags `json:",omitempty"`
}

func (lv *LogicalVolume[PhysicalVolume]) AddMapping(m Mapping) error {
	lv.init()
	// sanity check
	if _, haveDev := lv.id2pv[m.PAddr.Dev]; !haveDev {
		return fmt.Errorf("(%p).AddMapping: do not have a physical volume with id=%v",
			lv, m.PAddr.Dev)
	}

	// logical2physical
	newChunk := chunkMapping{
		LAddr:      m.LAddr,
		PAddrs:     []QualifiedPhysicalAddr{m.PAddr},
		Size:       m.Size,
		SizeLocked: m.SizeLocked,
		Flags:      m.Flags,
	}
	logicalOverlaps := lv.logical2physical.SearchRange(newChunk.cmpRange)
	var err error
	newChunk, err = newChunk.union(logicalOverlaps...)
	if err != nil {
		return fmt.Errorf("(%p).AddMapping: %w", lv, err)
	}

	// physical2logical
	newExt := devextMapping{
		PAddr:      m.PAddr.Addr,
		LAddr:      m.LAddr,
		Size:       m.Size,
		SizeLocked: m.SizeLocked,
		Flags:      m.Flags,
	}
	physicalOverlaps := lv.physical2logical[m.PAddr.Dev].SearchRange(newExt.cmpRange)
	newExt, err = newExt.union(physicalOverlaps...)
	if err != nil {
		return fmt.Errorf("(%p).AddMapping: %w", lv, err)
	}

	// optimize
	if len(logicalOverlaps) == 1 && reflect.DeepEqual(newChunk, logicalOverlaps[0]) &&
		len(physicalOverlaps) == 1 && reflect.DeepEqual(newExt, physicalOverlaps[0]) {
		return nil
	}

	// logical2physical
	for _, chunk := range logicalOverlaps {
		lv.logical2physical.Delete(containers.NativeOrdered[LogicalAddr]{Val: chunk.LAddr})
	}
	lv.logical2physical.Insert(newChunk)

	// physical2logical
	for _, ext := range physicalOverlaps {
		lv.physical2logical[m.PAddr.Dev].Delete(containers.NativeOrdered[PhysicalAddr]{Val: ext.PAddr})
	}
	lv.physical2logical[m.PAddr.Dev].Insert(newExt)

	// sanity check
	//
	// This is in-theory unnescessary, but that assumes that I
	// made no mistakes in my algorithm above.
	if os.Getenv("PARANOID") != "" {
		if err := lv.fsck(); err != nil {
			return err
		}
	}

	// done
	return nil
}

func (lv *LogicalVolume[PhysicalVolume]) fsck() error {
	physical2logical := make(map[DeviceID]*containers.RBTree[containers.NativeOrdered[PhysicalAddr], devextMapping])
	if err := lv.logical2physical.Walk(func(node *containers.RBNode[chunkMapping]) error {
		chunk := node.Value
		for _, stripe := range chunk.PAddrs {
			if _, devOK := lv.id2pv[stripe.Dev]; !devOK {
				return fmt.Errorf("(%p).fsck: chunk references physical volume %v which does not exist",
					lv, stripe.Dev)
			}
			if _, exists := physical2logical[stripe.Dev]; !exists {
				physical2logical[stripe.Dev] = &containers.RBTree[containers.NativeOrdered[PhysicalAddr], devextMapping]{
					KeyFn: func(ext devextMapping) containers.NativeOrdered[PhysicalAddr] {
						return containers.NativeOrdered[PhysicalAddr]{Val: ext.PAddr}
					},
				}
			}
			physical2logical[stripe.Dev].Insert(devextMapping{
				PAddr: stripe.Addr,
				LAddr: chunk.LAddr,
				Size:  chunk.Size,
				Flags: chunk.Flags,
			})
		}
		return nil
	}); err != nil {
		return err
	}

	if len(lv.physical2logical) != len(physical2logical) {
		return fmt.Errorf("(%p).fsck: skew between chunk tree and devext tree",
			lv)
	}
	for devid := range lv.physical2logical {
		if !lv.physical2logical[devid].Equal(physical2logical[devid]) {
			return fmt.Errorf("(%p).fsck: skew between chunk tree and devext tree",
				lv)
		}
	}

	return nil
}

func (lv *LogicalVolume[PhysicalVolume]) Mappings() []Mapping {
	var ret []Mapping
	_ = lv.logical2physical.Walk(func(node *containers.RBNode[chunkMapping]) error {
		chunk := node.Value
		var flags *BlockGroupFlags
		if chunk.Flags != nil {
			val := *chunk.Flags
			flags = &val
		}
		for _, slice := range chunk.PAddrs {
			ret = append(ret, Mapping{
				LAddr: chunk.LAddr,
				PAddr: slice,
				Size:  chunk.Size,
				Flags: flags,
			})
		}
		return nil
	})
	return ret
}

func (lv *LogicalVolume[PhysicalVolume]) Resolve(laddr LogicalAddr) (paddrs map[QualifiedPhysicalAddr]struct{}, maxlen AddrDelta) {
	node := lv.logical2physical.Search(func(chunk chunkMapping) int {
		return chunkMapping{LAddr: laddr, Size: 1}.cmpRange(chunk)
	})
	if node == nil {
		return nil, 0
	}

	chunk := node.Value

	offsetWithinChunk := laddr.Sub(chunk.LAddr)
	paddrs = make(map[QualifiedPhysicalAddr]struct{})
	maxlen = chunk.Size - offsetWithinChunk
	for _, stripe := range chunk.PAddrs {
		paddrs[QualifiedPhysicalAddr{
			Dev:  stripe.Dev,
			Addr: stripe.Addr.Add(offsetWithinChunk),
		}] = struct{}{}
	}

	return paddrs, maxlen
}

func (lv *LogicalVolume[PhysicalVolume]) ResolveAny(laddr LogicalAddr, size AddrDelta) (LogicalAddr, QualifiedPhysicalAddr) {
	node := lv.logical2physical.Search(func(chunk chunkMapping) int {
		return chunkMapping{LAddr: laddr, Size: size}.cmpRange(chunk)
	})
	if node == nil {
		return -1, QualifiedPhysicalAddr{0, -1}
	}
	return node.Value.LAddr, node.Value.PAddrs[0]
}

func (lv *LogicalVolume[PhysicalVolume]) UnResolve(paddr QualifiedPhysicalAddr) LogicalAddr {
	node := lv.physical2logical[paddr.Dev].Search(func(ext devextMapping) int {
		return devextMapping{PAddr: paddr.Addr, Size: 1}.cmpRange(ext)
	})
	if node == nil {
		return -1
	}

	ext := node.Value

	offsetWithinExt := paddr.Addr.Sub(ext.PAddr)
	return ext.LAddr.Add(offsetWithinExt)
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
		dev, ok := lv.id2pv[paddr.Dev]
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
		dev, ok := lv.id2pv[paddr.Dev]
		if !ok {
			return 0, fmt.Errorf("device=%v does not exist", paddr.Dev)
		}
		if _, err := dev.WriteAt(dat, paddr.Addr); err != nil {
			return 0, fmt.Errorf("write device=%v paddr=%v: %w", paddr.Dev, paddr.Addr, err)
		}
	}
	return len(dat), nil
}
