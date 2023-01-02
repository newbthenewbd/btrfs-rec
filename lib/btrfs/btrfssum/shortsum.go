// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum

import (
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"git.lukeshu.com/go/lowmemjson"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

const BlockSize = 4 * 1024

// ShortSum //////////////////////////////////////////////////////////

type ShortSum string

var (
	_ lowmemjson.Encodable = ShortSum("")
	_ lowmemjson.Decodable = (*ShortSum)(nil)
)

func (sum ShortSum) ToFullSum() CSum {
	var ret CSum
	copy(ret[:], sum)
	return ret
}

func (sum ShortSum) EncodeJSON(w io.Writer) error {
	const hextable = "0123456789abcdef"
	var buf [2]byte
	buf[0] = '"'
	if _, err := w.Write(buf[:1]); err != nil {
		return err
	}
	for i := 0; i < len(sum); i++ {
		buf[0] = hextable[sum[i]>>4]
		buf[1] = hextable[sum[i]&0x0f]
		if _, err := w.Write(buf[:]); err != nil {
			return err
		}
	}
	buf[0] = '"'
	if _, err := w.Write(buf[:1]); err != nil {
		return err
	}
	return nil
}

func deHex(r rune) (byte, bool) {
	if r > math.MaxUint8 {
		return 0, false
	}
	c := byte(r)
	//nolint:gomnd // Hex conversion.
	switch {
	case '0' <= c && c <= '9':
		return c - '0', true
	case 'a' <= c && c <= 'f':
		return c - 'a' + 10, true
	case 'A' <= c && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

func (sum *ShortSum) DecodeJSON(r io.RuneScanner) error {
	var out strings.Builder
	if c, _, err := r.ReadRune(); err != nil {
		return err
	} else if c != '"' {
		return fmt.Errorf("expected %q, got %q", '"', c)
	}
	for {
		a, _, err := r.ReadRune()
		if err != nil {
			return err
		}
		if a == '"' {
			break
		}
		aN, ok := deHex(a)
		if !ok {
			return fmt.Errorf("expected a hex digit, got %q", a)
		}
		b, _, err := r.ReadRune()
		if err != nil {
			return err
		}
		bN, ok := deHex(b)
		if !ok {
			return fmt.Errorf("expected a hex digit, got %q", b)
		}
		out.WriteByte(aN<<4 | bN)
	}
	*sum = ShortSum(out.String())
	return nil
}

// SumRun ////////////////////////////////////////////////////////////

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
	return ShortSum(run.Sums[off : off+run.ChecksumSize]), nil
}

func (run SumRun[Addr]) SumForAddr(addr Addr) (ShortSum, bool) {
	if addr < run.Addr || addr >= run.Addr.Add(run.Size()) {
		return "", false
	}
	off := int((addr-run.Addr)/BlockSize) * run.ChecksumSize
	return ShortSum(run.Sums[off : off+run.ChecksumSize]), true
}

func (run SumRun[Addr]) Walk(ctx context.Context, fn func(Addr, ShortSum) error) error {
	for addr, off := run.Addr, 0; off < len(run.Sums); addr, off = addr+BlockSize, off+run.ChecksumSize {
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
		return ShortSum(run.Sums[off : off+run.ChecksumSize]), nil
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
			if err := lowmemjson.Encode(w, run); err != nil {
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
			return lowmemjson.Decode(r, &name)
		},
		func(r io.RuneScanner) error {
			switch name {
			case "Addr":
				return lowmemjson.Decode(r, &sg.Addr)
			case "Size":
				return lowmemjson.Decode(r, &sg.Size)
			case "Runs":
				return lowmemjson.DecodeArray(r, func(r io.RuneScanner) error {
					var run SumRun[Addr]
					if err := lowmemjson.Decode(r, &run); err != nil {
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
