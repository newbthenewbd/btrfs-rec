// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum

import (
	"context"
	"fmt"
	"io"
	"math"

	"git.lukeshu.com/go/lowmemjson"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type SumRunWithGaps[Addr btrfsvol.IntAddr[Addr]] struct {
	// Store the start address and size, in order to facilitate
	// leading and trailing gaps.
	Addr Addr
	Size btrfsvol.AddrDelta

	Runs []SumRun[Addr]
}

var (
	_ lowmemjson.Encodable = SumRunWithGaps[btrfsvol.LogicalAddr]{}
	_ lowmemjson.Decodable = (*SumRunWithGaps[btrfsvol.LogicalAddr])(nil)
)

func (sg SumRunWithGaps[Addr]) NumSums() int {
	return int(sg.Size / BlockSize)
}

func (sg SumRunWithGaps[Addr]) PctFull() float64 {
	total := sg.NumSums()
	var full int
	for _, run := range sg.Runs {
		full += run.NumSums()
	}
	return float64(full) / float64(total)
}

func (sg SumRunWithGaps[Addr]) RunForAddr(addr Addr) (SumRun[Addr], Addr, bool) {
	for _, run := range sg.Runs {
		if run.Addr > addr {
			return SumRun[Addr]{}, run.Addr, false
		}
		if run.Addr.Add(run.Size()) <= addr {
			continue
		}
		return run, 0, true
	}
	return SumRun[Addr]{}, math.MaxInt64, false
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
		off := int((addr-run.Addr)/BlockSize) * run.ChecksumSize
		return run.Sums[off : off+run.ChecksumSize], nil
	}
	return "", diskio.ErrWildcard
}

func (sg SumRunWithGaps[Addr]) Walk(ctx context.Context, fn func(Addr, ShortSum) error) error {
	for _, run := range sg.Runs {
		if err := run.Walk(ctx, fn); err != nil {
			return err
		}
	}
	return nil
}

// Get implements diskio.Sequence[int, ShortSum]
func (sg SumRunWithGaps[Addr]) Get(sumIdx int64) (ShortSum, error) {
	addr := sg.Addr.Add(btrfsvol.AddrDelta(sumIdx) * BlockSize)
	return sg.SumForAddr(addr)
}

func (sg SumRunWithGaps[Addr]) EncodeJSON(w io.Writer) error {
	if _, err := fmt.Fprintf(w, `{"Addr":%d,"Size":%d,"Runs":[`, sg.Addr, sg.Size); err != nil {
		return err
	}
	cur := sg.Addr
	for i, run := range sg.Runs {
		if i > 0 {
			if _, err := w.Write([]byte{','}); err != nil {
				return err
			}
		}
		switch {
		case run.Addr < cur:
			return fmt.Errorf("invalid %T: addr went backwards: %v < %v", sg, run.Addr, cur)
		case run.Addr > cur:
			if _, err := fmt.Fprintf(w, `{"Gap":%d},`, run.Addr.Sub(cur)); err != nil {
				return err
			}
			fallthrough
		default:
			if err := lowmemjson.NewEncoder(w).Encode(run); err != nil {
				return err
			}
			cur = run.Addr.Add(run.Size())
		}
	}
	end := sg.Addr.Add(sg.Size)
	switch {
	case end < cur:
		return fmt.Errorf("invalid %T: addr went backwards: %v < %v", sg, end, cur)
	case end > cur:
		if _, err := fmt.Fprintf(w, `,{"Gap":%d}`, end.Sub(cur)); err != nil {
			return err
		}
	}
	if _, err := w.Write([]byte("]}")); err != nil {
		return err
	}
	return nil
}

func (sg *SumRunWithGaps[Addr]) DecodeJSON(r io.RuneScanner) error {
	*sg = SumRunWithGaps[Addr]{}
	var name string
	return lowmemjson.DecodeObject(r,
		func(r io.RuneScanner) error {
			return lowmemjson.NewDecoder(r).Decode(&name)
		},
		func(r io.RuneScanner) error {
			switch name {
			case "Addr":
				return lowmemjson.NewDecoder(r).Decode(&sg.Addr)
			case "Size":
				return lowmemjson.NewDecoder(r).Decode(&sg.Size)
			case "Runs":
				return lowmemjson.DecodeArray(r, func(r io.RuneScanner) error {
					var run SumRun[Addr]
					if err := lowmemjson.NewDecoder(r).Decode(&run); err != nil {
						return err
					}
					if run.ChecksumSize > 0 {
						sg.Runs = append(sg.Runs, run)
					}
					return nil
				})
			default:
				return fmt.Errorf("unknown key %q", name)
			}
		})
}
