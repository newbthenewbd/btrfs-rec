// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"encoding/json"
)

type Optional[T any] struct {
	OK  bool
	Val T
}

var (
	_ json.Marshaler   = Optional[bool]{}
	_ json.Unmarshaler = (*Optional[bool])(nil)
)

func (o Optional[T]) MarshalJSON() ([]byte, error) {
	if !o.OK {
		return []byte("null"), nil
	}
	return json.Marshal(o.Val)
}

func (o *Optional[T]) UnmarshalJSON(dat []byte) error {
	if string(dat) == "null" {
		*o = Optional[T]{}
		return nil
	}
	o.OK = true
	return json.Unmarshal(dat, &o.Val)
}
