// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package jsonutil

import (
	"fmt"
	"io"
	"math"
)

type invalidHexRuneError rune

func (e invalidHexRuneError) Error() string {
	return fmt.Sprintf("jsonutil: invalid hex digit: %q", rune(e))
}

// hexDecoder is like an encoding/hex.Decoder, but has a "push"
// interface rather than a "pull" interface.
type hexDecoder struct {
	dst io.ByteWriter

	buf   byte
	bufOK bool
}

func (d *hexDecoder) WriteRune(r rune) (int, error) {
	if r > math.MaxUint8 {
		return 0, invalidHexRuneError(r)
	}

	c := byte(r)
	var v byte
	//nolint:gomnd // Hex conversion.
	switch {
	case '0' <= c && c <= '9':
		v = c - '0'
	case 'a' <= c && c <= 'f':
		v = c - 'a' + 10
	case 'A' <= c && c <= 'F':
		v = c - 'A' + 10
	default:
		return 0, invalidHexRuneError(r)
	}

	if !d.bufOK {
		d.buf = v
		d.bufOK = true
		return 1, nil
	}
	d.bufOK = false
	return 1, d.dst.WriteByte(d.buf<<4 | v)
}

func (d *hexDecoder) Close() error {
	if d.bufOK {
		return io.ErrUnexpectedEOF
	}
	return nil
}
