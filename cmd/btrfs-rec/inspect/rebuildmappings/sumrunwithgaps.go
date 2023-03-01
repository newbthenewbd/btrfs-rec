// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"context"
	"fmt"
	"io"
	"math"

	"git.lukeshu.com/go/lowmemjson"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

type sumRunWithGaps[Addr btrfsvol.IntAddr[Addr]] struct {
	// Store the start address and size, in order to facilitate
	// leading and trailing gaps.
	Addr Addr
	Size btrfsvol.AddrDelta

	Runs []btrfssum.SumRun[Addr]
}

var (
	_ lowmemjson.Encodable = sumRunWithGaps[btrfsvol.LogicalAddr]{}
	_ lowmemjson.Decodable = (*sumRunWithGaps[btrfsvol.LogicalAddr])(nil)
)

// PatLen implements kmpPattern[int, ShortSum].
func (sg sumRunWithGaps[Addr]) PatLen() int {
	return int(sg.Size / btrfssum.BlockSize)
}

func (sg sumRunWithGaps[Addr]) PctFull() float64 {
	total := sg.PatLen()
	var full int
	for _, run := range sg.Runs {
		full += run.SeqLen()
	}
	return float64(full) / float64(total)
}

func (sg sumRunWithGaps[Addr]) RunForAddr(addr Addr) (btrfssum.SumRun[Addr], Addr, bool) {
	for _, run := range sg.Runs {
		if run.Addr > addr {
			return btrfssum.SumRun[Addr]{}, run.Addr, false
		}
		if run.Addr.Add(run.Size()) <= addr {
			continue
		}
		return run, 0, true
	}
	return btrfssum.SumRun[Addr]{}, math.MaxInt64, false
}

func (sg sumRunWithGaps[Addr]) SumForAddr(addr Addr) (btrfssum.ShortSum, bool) {
	if addr < sg.Addr || addr >= sg.Addr.Add(sg.Size) {
		return "", false
	}
	runIdx, ok := slices.Search(sg.Runs, func(run btrfssum.SumRun[Addr]) int {
		switch {
		case addr < run.Addr:
			return -1
		case addr >= run.Addr.Add(run.Size()):
			return 1
		default:
			return 0
		}
	})
	if !ok {
		return "", false
	}
	run := sg.Runs[runIdx]
	off := int((addr-run.Addr)/btrfssum.BlockSize) * run.ChecksumSize
	return run.Sums[off : off+run.ChecksumSize], true
}

func (sg sumRunWithGaps[Addr]) Walk(ctx context.Context, fn func(Addr, btrfssum.ShortSum) error) error {
	for _, run := range sg.Runs {
		if err := run.Walk(ctx, fn); err != nil {
			return err
		}
	}
	return nil
}

// PatGet implements kmpPattern[int, ShortSum].
func (sg sumRunWithGaps[Addr]) PatGet(sumIdx int) (btrfssum.ShortSum, bool) {
	addr := sg.Addr.Add(btrfsvol.AddrDelta(sumIdx) * btrfssum.BlockSize)
	return sg.SumForAddr(addr)
}

func (sg sumRunWithGaps[Addr]) EncodeJSON(w io.Writer) error {
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

func (sg *sumRunWithGaps[Addr]) DecodeJSON(r io.RuneScanner) error {
	*sg = sumRunWithGaps[Addr]{}
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
					var run btrfssum.SumRun[Addr]
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
