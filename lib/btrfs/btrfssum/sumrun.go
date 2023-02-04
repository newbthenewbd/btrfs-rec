// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum

import (
	"context"
	"io"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type SumRun[Addr btrfsvol.IntAddr[Addr]] struct {
	// How big a ShortSum is in this Run.
	ChecksumSize int `json:",omitempty"`
	// Base address where this run starts.
	Addr Addr `json:",omitempty"`
	// All of the ShortSums in this run, concatenated together.
	Sums ShortSum
}

func (run SumRun[Addr]) NumSums() int {
	return len(run.Sums) / run.ChecksumSize
}

func (run SumRun[Addr]) Size() btrfsvol.AddrDelta {
	return btrfsvol.AddrDelta(run.NumSums()) * BlockSize
}

// Get implements diskio.Sequence[int, ShortSum]
func (run SumRun[Addr]) Get(sumIdx int64) (ShortSum, error) {
	if sumIdx < 0 || int(sumIdx) >= run.NumSums() {
		return "", io.EOF
	}
	off := int(sumIdx) * run.ChecksumSize
	return run.Sums[off : off+run.ChecksumSize], nil
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
