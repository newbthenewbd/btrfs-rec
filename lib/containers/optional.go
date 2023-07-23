// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"io"

	"git.lukeshu.com/go/lowmemjson"
)

type Optional[T any] struct {
	OK  bool
	Val T
}

func OptionalValue[T any](val T) Optional[T] {
	return Optional[T]{
		OK:  true,
		Val: val,
	}
}

func OptionalNil[T any]() Optional[T] {
	return Optional[T]{
		OK: false,
	}
}

var (
	_ lowmemjson.Encodable = Optional[bool]{}
	_ lowmemjson.Decodable = (*Optional[bool])(nil)
)

func (o Optional[T]) EncodeJSON(w io.Writer) error {
	if !o.OK {
		_, err := io.WriteString(w, "null")
		return err
	}
	return lowmemjson.NewEncoder(w).Encode(o.Val)
}

func (o *Optional[T]) DecodeJSON(r io.RuneScanner) error {
	c, _, _ := r.ReadRune()
	if c == 'n' {
		_, _, _ = r.ReadRune() // u
		_, _, _ = r.ReadRune() // l
		_, _, _ = r.ReadRune() // l
		*o = Optional[T]{}
		return nil
	}
	_ = r.UnreadRune()
	o.OK = true
	return lowmemjson.NewDecoder(r).Decode(&o.Val)
}
