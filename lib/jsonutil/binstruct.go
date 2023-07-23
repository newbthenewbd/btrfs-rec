// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package jsonutil

import (
	"bytes"
	"fmt"
	"io"

	"git.lukeshu.com/go/lowmemjson"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type Binary[T any] struct {
	Val T
}

var (
	_ lowmemjson.Encodable = Binary[int]{}
	_ lowmemjson.Decodable = (*Binary[int])(nil)
)

func (o Binary[T]) EncodeJSON(w io.Writer) error {
	bs, err := binstruct.Marshal(o.Val)
	if err != nil {
		return err
	}
	return EncodeSplitHexString(w, bs, textui.Tunable(80))
}

func (o *Binary[T]) DecodeJSON(r io.RuneScanner) error {
	var buf bytes.Buffer
	if err := DecodeSplitHexString(r, &buf); err != nil {
		return err
	}
	n, err := binstruct.Unmarshal(buf.Bytes(), &o.Val)
	if err != nil {
		return err
	}
	if n < buf.Len() {
		return fmt.Errorf("%d bytes of garbage after value", n-buf.Len())
	}
	return nil
}
