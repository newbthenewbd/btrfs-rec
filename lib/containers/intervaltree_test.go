// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (t *IntervalTree[K, V]) ASCIIArt() string {
	return t.inner.ASCIIArt()
}

func (v intervalValue[K, V]) String() string {
	return fmt.Sprintf("%v) ([%v,%v]",
		v.Val,
		v.SpanOfChildren.Min,
		v.SpanOfChildren.Max)
}

func (v NativeOrdered[T]) String() string {
	return fmt.Sprintf("%v", v.Val)
}

type SimpleInterval struct {
	Min, Max int
}

func (ival SimpleInterval) String() string {
	return fmt.Sprintf("[%v,%v]", ival.Min, ival.Max)
}

func TestIntervalTree(t *testing.T) {
	tree := IntervalTree[NativeOrdered[int], SimpleInterval]{
		MinFn: func(ival SimpleInterval) NativeOrdered[int] { return NativeOrdered[int]{ival.Min} },
		MaxFn: func(ival SimpleInterval) NativeOrdered[int] { return NativeOrdered[int]{ival.Max} },
	}

	// CLRS Figure 14.4
	// level 0
	tree.Insert(SimpleInterval{16, 21})
	// level 1
	tree.Insert(SimpleInterval{8, 9})
	tree.Insert(SimpleInterval{25, 30})
	// level 2
	tree.Insert(SimpleInterval{5, 8})
	tree.Insert(SimpleInterval{15, 23})
	tree.Insert(SimpleInterval{17, 19})
	tree.Insert(SimpleInterval{26, 26})
	// level 3
	tree.Insert(SimpleInterval{0, 3})
	tree.Insert(SimpleInterval{6, 10})
	tree.Insert(SimpleInterval{19, 20})

	t.Log(tree.ASCIIArt())

	// find intervals that touch [9,20]
	intervals := tree.SearchAll(func(k NativeOrdered[int]) int {
		if k.Val < 9 {
			return 1
		}
		if k.Val > 20 {
			return -1
		}
		return 0
	})
	assert.Equal(t,
		[]SimpleInterval{
			{6, 10},
			{8, 9},
			{15, 23},
			{16, 21},
			{17, 19},
			{19, 20},
		},
		intervals)
}
