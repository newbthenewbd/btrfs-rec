// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"io"

	"git.lukeshu.com/go/lowmemjson"
	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

type Set[T constraints.Ordered] map[T]struct{}

var (
	_ lowmemjson.Encodable = Set[int]{}
	_ lowmemjson.Decodable = (*Set[int])(nil)
)

func (o Set[T]) EncodeJSON(w io.Writer) error {
	return lowmemjson.Encode(w, maps.SortedKeys(o))
}

func (o *Set[T]) DecodeJSON(r io.RuneScanner) error {
	c, _, _ := r.ReadRune()
	if c == 'n' {
		_, _, _ = r.ReadRune() // u
		_, _, _ = r.ReadRune() // l
		_, _, _ = r.ReadRune() // l
		*o = nil
		return nil
	}
	_ = r.UnreadRune()
	*o = Set[T]{}
	return lowmemjson.DecodeArray(r, func(r io.RuneScanner) error {
		var val T
		if err := lowmemjson.Decode(r, &val); err != nil {
			return err
		}
		(*o)[val] = struct{}{}
		return nil
	})
}

func (o Set[T]) Insert(v T) {
	o[v] = struct{}{}
}

func (o Set[T]) InsertFrom(p Set[T]) {
	for v := range p {
		o[v] = struct{}{}
	}
}

func (o Set[T]) Delete(v T) {
	if o == nil {
		return
	}
	delete(o, v)
}

func (o Set[T]) DeleteFrom(p Set[T]) {
	if o == nil {
		return
	}
	for v := range p {
		delete(o, v)
	}
}

func (o Set[T]) TakeOne() T {
	for v := range o {
		return v
	}
	var zero T
	return zero
}

func (small Set[T]) HasIntersection(big Set[T]) bool {
	if len(big) < len(small) {
		small, big = big, small
	}
	for v := range small {
		if _, ok := big[v]; ok {
			return true
		}
	}
	return false
}
