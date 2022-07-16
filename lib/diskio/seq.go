// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"fmt"
	"io"
)

// interface /////////////////////////////////////////////////////////

type Sequence[K ~int64, V any] interface {
	// Get the value at 'pos' in the sequence.  Positions start at
	// 0 and increment naturally.  Return an error that is io.EOF
	// if 'pos' is past the end of the sequence'.
	Get(pos K) (V, error)
}

// implementation: slice /////////////////////////////////////////////

type SliceSequence[K ~int64, V any] []V

var _ Sequence[assertAddr, byte] = SliceSequence[assertAddr, byte]([]byte(nil))

func (s SliceSequence[K, V]) Get(i K) (V, error) {
	if i >= K(len(s)) {
		var v V
		return v, io.EOF
	}
	return s[int(i)], nil
}

// implementation: string ////////////////////////////////////////////

type StringSequence[K ~int64] string

var _ Sequence[assertAddr, byte] = StringSequence[assertAddr]("")

func (s StringSequence[K]) Get(i K) (byte, error) {
	if i >= K(len(s)) {
		return 0, io.EOF
	}
	return s[int(i)], nil
}

// implementation: io.ByteReader /////////////////////////////////////

type ByteReaderSequence[K ~int64] struct {
	R   io.ByteReader
	pos K
}

var _ Sequence[assertAddr, byte] = &ByteReaderSequence[assertAddr]{R: nil}

func (s *ByteReaderSequence[K]) Get(i K) (byte, error) {
	if i != s.pos {
		return 0, fmt.Errorf("%T.Get(%v): can only call .Get(%v)",
			s, i, s.pos)
	}
	chr, err := s.R.ReadByte()
	if err != nil {
		return chr, err
	}
	s.pos++
	return chr, nil
}
