// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package jsonutil provides utilities for implementing the interfaces
// consumed by the "git.lukeshu.com/go/lowmemjson" package.
package jsonutil

import (
	"io"

	"git.lukeshu.com/go/lowmemjson"
)

func EncodeHexString[T ~[]byte | ~string](w io.Writer, str T) error {
	const hextable = "0123456789abcdef"
	var buf [2]byte
	buf[0] = '"'
	if _, err := w.Write(buf[:1]); err != nil {
		return err
	}
	for i := 0; i < len(str); i++ {
		buf[0] = hextable[str[i]>>4]
		buf[1] = hextable[str[i]&0x0f]
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

func DecodeHexString(r io.RuneScanner, dst io.ByteWriter) error {
	dec := &hexDecoder{dst: dst}
	if err := lowmemjson.DecodeString(r, dec); err != nil {
		return err
	}
	return dec.Close()
}
