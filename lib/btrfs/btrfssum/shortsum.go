// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum

import (
	"fmt"
	"io"
	"math"
	"strings"

	"git.lukeshu.com/go/lowmemjson"
)

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
