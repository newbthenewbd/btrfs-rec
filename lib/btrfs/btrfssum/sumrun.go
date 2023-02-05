// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum

import (
	"context"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type SumRun[Addr btrfsvol.IntAddr[Addr]] struct {
	// How big a ShortSum is in this Run.
	ChecksumSize int `json:",omitempty"`
	// Base address where this run starts.
	Addr Addr `json:",omitempty"`
	// All of the ShortSums in this run, concatenated together.
	Sums ShortSum
}

var _ diskio.Sequence[int, ShortSum] = SumRun[btrfsvol.LogicalAddr]{}

// SeqLen implements diskio.Sequence[int, ShortSum].
func (run SumRun[Addr]) SeqLen() int {
	return len(run.Sums) / run.ChecksumSize
}

func (run SumRun[Addr]) Size() btrfsvol.AddrDelta {
	return btrfsvol.AddrDelta(run.SeqLen()) * BlockSize
}

// SeqGet implements diskio.Sequence[int, ShortSum].
func (run SumRun[Addr]) SeqGet(sumIdx int) ShortSum {
	off := sumIdx * run.ChecksumSize
	return run.Sums[off : off+run.ChecksumSize]
}

func (run SumRun[Addr]) SumForAddr(addr Addr) (ShortSum, bool) {
	if addr < run.Addr || addr >= run.Addr.Add(run.Size()) {
		return "", false
	}
	off := int((addr-run.Addr)/BlockSize) * run.ChecksumSize
	return run.Sums[off : off+run.ChecksumSize], true
}

func (run SumRun[Addr]) Walk(ctx context.Context, fn func(Addr, ShortSum) error) error {
	for addr, off := run.Addr, 0; off < len(run.Sums); addr, off = addr+BlockSize, off+run.ChecksumSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(addr, run.Sums[off:off+run.ChecksumSize]); err != nil {
			return err
		}
	}
	return nil
}
