// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsinspect

import (
	"context"
	"io"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

// ShortSum //////////////////////////////////////////////////////////

type ShortSum string

// SumRun ////////////////////////////////////////////////////////////

type SumRun[Addr btrfsvol.IntAddr[Addr]] struct {
	// How big a ShortSum is in this Run.
	ChecksumSize int
	// Base address where this run starts.
	Addr Addr
	// All of the ShortSums in this run, concatenated together.
	//
	// This is a 'string' rather than a 'ShortSum' to make it hard
	// to accidentally use it as a single sum.
	Sums string
}

func (run SumRun[Addr]) NumSums() int {
	return len(run.Sums) / run.ChecksumSize
}

func (run SumRun[Addr]) Size() btrfsvol.AddrDelta {
	return btrfsvol.AddrDelta(run.NumSums()) * btrfsitem.CSumBlockSize
}

// Get implements diskio.Sequence[int, ShortSum]
func (run SumRun[Addr]) Get(sumIdx int64) (ShortSum, error) {
	if sumIdx < 0 || int(sumIdx) >= run.NumSums() {
		return "", io.EOF
	}
	off := int(sumIdx) * run.ChecksumSize
	return ShortSum(run.Sums[off : off+run.ChecksumSize]), nil
}

func (run SumRun[Addr]) SumForAddr(addr Addr) (ShortSum, bool) {
	if addr < run.Addr || addr >= run.Addr.Add(run.Size()) {
		return "", false
	}
	off := int((addr-run.Addr)/btrfsitem.CSumBlockSize) * run.ChecksumSize
	return ShortSum(run.Sums[off : off+run.ChecksumSize]), true
}

func (run SumRun[Addr]) Walk(ctx context.Context, fn func(Addr, ShortSum) error) error {
	for addr, off := run.Addr, 0; off < len(run.Sums); addr, off = addr+btrfsitem.CSumBlockSize, off+run.ChecksumSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(addr, ShortSum(run.Sums[off:off+run.ChecksumSize])); err != nil {
			return err
		}
	}
	return nil
}

// SumRunWithGaps ////////////////////////////////////////////////////

type SumRunWithGaps[Addr btrfsvol.IntAddr[Addr]] struct {
	Addr Addr
	Size btrfsvol.AddrDelta
	Runs []SumRun[Addr]
}

func (sg SumRunWithGaps[Addr]) NumSums() int {
	return int(sg.Size / btrfsitem.CSumBlockSize)
}

func (sg SumRunWithGaps[Addr]) PctFull() float64 {
	total := sg.NumSums()
	var full int
	for _, run := range sg.Runs {
		full += run.NumSums()
	}
	return float64(full) / float64(total)
}

func (sg SumRunWithGaps[Addr]) SumForAddr(addr Addr) (ShortSum, error) {
	if addr < sg.Addr || addr >= sg.Addr.Add(sg.Size) {
		return "", io.EOF
	}
	for _, run := range sg.Runs {
		if run.Addr > addr {
			return "", diskio.ErrWildcard
		}
		if run.Addr.Add(run.Size()) <= addr {
			continue
		}
		off := int((addr-run.Addr)/btrfsitem.CSumBlockSize) * run.ChecksumSize
		return ShortSum(run.Sums[off : off+run.ChecksumSize]), nil
	}
	return "", diskio.ErrWildcard
}

// Get implements diskio.Sequence[int, ShortSum]
func (sg SumRunWithGaps[Addr]) Get(sumIdx int64) (ShortSum, error) {
	addr := sg.Addr.Add(btrfsvol.AddrDelta(sumIdx) * btrfsitem.CSumBlockSize)
	return sg.SumForAddr(addr)
}
