// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

// interface /////////////////////////////////////////////////////////

type Sequence[K ~int64 | ~int, V any] interface {
	SeqLen() K
	// Get the value at 'pos' in the sequence.  Positions start at
	// 0 and increment naturally.  It is invalid to call
	// SeqGet(pos) with a pos that is >= SeqLen().
	SeqGet(pos K) V
}

// implementation: slice /////////////////////////////////////////////

type SliceSequence[K ~int64 | ~int, V any] []V

var _ Sequence[assertAddr, byte] = SliceSequence[assertAddr, byte](nil)

func (s SliceSequence[K, V]) SeqLen() K {
	return K(len(s))
}

func (s SliceSequence[K, V]) SeqGet(i K) V {
	return s[int(i)]
}

// implementation: string ////////////////////////////////////////////

type StringSequence[K ~int64 | ~int] string

var _ Sequence[assertAddr, byte] = StringSequence[assertAddr]("")

func (s StringSequence[K]) SeqLen() K {
	return K(len(s))
}

func (s StringSequence[K]) SeqGet(i K) byte {
	return s[int(i)]
}
