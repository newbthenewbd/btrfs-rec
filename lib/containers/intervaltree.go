// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

type interval[K Ordered[K]] struct {
	Min, Max K
}

// Compare implements Ordered.
func (a interval[K]) Compare(b interval[K]) int {
	if d := a.Min.Compare(b.Min); d != 0 {
		return d
	}
	return a.Max.Compare(b.Max)
}

// ContainsFn returns whether this interval contains the range matched
// by the given function.
func (ival interval[K]) ContainsFn(fn func(K) int) bool {
	return fn(ival.Min) >= 0 && fn(ival.Max) <= 0
}

type intervalValue[K Ordered[K], V any] struct {
	Val       V
	ValSpan   interval[K]
	ChildSpan interval[K]
}

// Compare implements Ordered.
func (a intervalValue[K, V]) Compare(b intervalValue[K, V]) int {
	return a.ValSpan.Compare(b.ValSpan)
}

type IntervalTree[K Ordered[K], V any] struct {
	MinFn func(V) K
	MaxFn func(V) K
	inner RBTree[intervalValue[K, V]]
}

func (t *IntervalTree[K, V]) attrFn(node *RBNode[intervalValue[K, V]]) {
	max := node.Value.ValSpan.Max
	if node.Left != nil && node.Left.Value.ChildSpan.Max.Compare(max) > 0 {
		max = node.Left.Value.ChildSpan.Max
	}
	if node.Right != nil && node.Right.Value.ChildSpan.Max.Compare(max) > 0 {
		max = node.Right.Value.ChildSpan.Max
	}
	node.Value.ChildSpan.Max = max

	min := node.Value.ValSpan.Min
	if node.Left != nil && node.Left.Value.ChildSpan.Min.Compare(min) < 0 {
		min = node.Left.Value.ChildSpan.Min
	}
	if node.Right != nil && node.Right.Value.ChildSpan.Min.Compare(min) < 0 {
		min = node.Right.Value.ChildSpan.Min
	}
	node.Value.ChildSpan.Min = min
}

func (t *IntervalTree[K, V]) init() {
	if t.inner.AttrFn == nil {
		t.inner.AttrFn = t.attrFn
	}
}

func (t *IntervalTree[K, V]) Equal(u *IntervalTree[K, V]) bool {
	return t.inner.Equal(&u.inner)
}

func (t *IntervalTree[K, V]) Insert(val V) {
	t.init()
	t.inner.Insert(intervalValue[K, V]{
		Val: val,
		ValSpan: interval[K]{
			Min: t.MinFn(val),
			Max: t.MaxFn(val),
		},
	})
}

func (t *IntervalTree[K, V]) Min() (K, bool) {
	if t.inner.root == nil {
		var zero K
		return zero, false
	}
	return t.inner.root.Value.ChildSpan.Min, true
}

func (t *IntervalTree[K, V]) Max() (K, bool) {
	if t.inner.root == nil {
		var zero K
		return zero, false
	}
	return t.inner.root.Value.ChildSpan.Max, true
}

func (t *IntervalTree[K, V]) Search(fn func(K) int) (V, bool) {
	node := t.inner.root
	for node != nil {
		switch {
		case node.Value.ValSpan.ContainsFn(fn):
			return node.Value.Val, true
		case node.Left != nil && node.Left.Value.ChildSpan.ContainsFn(fn):
			node = node.Left
		case node.Right != nil && node.Right.Value.ChildSpan.ContainsFn(fn):
			node = node.Right
		default:
			node = nil
		}
	}
	var zero V
	return zero, false
}

func (t *IntervalTree[K, V]) Range(fn func(V) bool) {
	t.inner.Range(func(node *RBNode[intervalValue[K, V]]) bool {
		return fn(node.Value.Val)
	})
}

func (t *IntervalTree[K, V]) Subrange(rangeFn func(K) int, handleFn func(V) bool) {
	t.subrange(t.inner.root, rangeFn, handleFn)
}

func (t *IntervalTree[K, V]) subrange(node *RBNode[intervalValue[K, V]], rangeFn func(K) int, handleFn func(V) bool) bool {
	if node == nil {
		return true
	}
	if !node.Value.ChildSpan.ContainsFn(rangeFn) {
		return true
	}
	if !t.subrange(node.Left, rangeFn, handleFn) {
		return false
	}
	if node.Value.ValSpan.ContainsFn(rangeFn) {
		if !handleFn(node.Value.Val) {
			return false
		}
	}
	if !t.subrange(node.Right, rangeFn, handleFn) {
		return false
	}
	return true
}
