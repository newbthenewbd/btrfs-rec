// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"fmt"
	"io"
	"sort"

	"git.lukeshu.com/go/lowmemjson"

	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
)

// Set[T] is an unordered set of T.
type Set[T comparable] map[T]struct{}

var (
	_ lowmemjson.Encodable = Set[int]{}
	_ lowmemjson.Decodable = (*Set[int])(nil)
)

func cast[T any](x any) T { return x.(T) }

func (o Set[T]) EncodeJSON(w io.Writer) error {
	var less func(a, b T) bool
	var zero T
	switch (any(zero)).(type) {
	case _Ordered[T]:
		less = func(a, b T) bool { return cast[_Ordered[T]](a).Cmp(b) < 0 }
	// This is the constraints.Ordered list
	case string:
		less = func(a, b T) bool { return cast[string](a) < cast[string](b) }
	case int:
		less = func(a, b T) bool { return cast[int](a) < cast[int](b) }
	case int8:
		less = func(a, b T) bool { return cast[int8](a) < cast[int8](b) }
	case int16:
		less = func(a, b T) bool { return cast[int16](a) < cast[int16](b) }
	case int32:
		less = func(a, b T) bool { return cast[int32](a) < cast[int32](b) }
	case int64:
		less = func(a, b T) bool { return cast[int64](a) < cast[int64](b) }
	case uint:
		less = func(a, b T) bool { return cast[uint](a) < cast[uint](b) }
	case uint8:
		less = func(a, b T) bool { return cast[uint8](a) < cast[uint8](b) }
	case uint16:
		less = func(a, b T) bool { return cast[uint16](a) < cast[uint16](b) }
	case uint32:
		less = func(a, b T) bool { return cast[uint32](a) < cast[uint32](b) }
	case uint64:
		less = func(a, b T) bool { return cast[uint64](a) < cast[uint64](b) }
	case uintptr:
		less = func(a, b T) bool { return cast[uintptr](a) < cast[uintptr](b) }
	case float32:
		less = func(a, b T) bool { return cast[float32](a) < cast[float32](b) }
	case float64:
		less = func(a, b T) bool { return cast[float64](a) < cast[float64](b) }
	default:
		less = func(a, b T) bool { return fmt.Sprint(a) < fmt.Sprint(b) }
	}

	keys := maps.Keys(o)
	sort.Slice(keys, func(i, j int) bool {
		return less(keys[i], keys[j])
	})

	return lowmemjson.Encode(w, keys)
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

func NewSet[T comparable](values ...T) Set[T] {
	ret := make(Set[T], len(values))
	for _, value := range values {
		ret.Insert(value)
	}
	return ret
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

func (o Set[T]) Has(v T) bool {
	_, has := o[v]
	return has
}

func (small Set[T]) HasAny(big Set[T]) bool {
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

func (small Set[T]) Intersection(big Set[T]) Set[T] {
	if len(big) < len(small) {
		small, big = big, small
	}
	ret := make(Set[T])
	for v := range small {
		if _, ok := big[v]; ok {
			ret.Insert(v)
		}
	}
	return ret
}
