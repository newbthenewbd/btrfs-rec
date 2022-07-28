// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

type intervalKey[K Ordered[K]] struct {
	Min, Max K
}

func (ival intervalKey[K]) ContainsFn(fn func(K) int) bool {
	return fn(ival.Min) >= 0 && fn(ival.Max) <= 0
}

func (a intervalKey[K]) Cmp(b intervalKey[K]) int {
	if d := a.Min.Cmp(b.Min); d != 0 {
		return d
	}
	return a.Max.Cmp(b.Max)
}

type intervalValue[K Ordered[K], V any] struct {
	Val            V
	SpanOfChildren intervalKey[K]
}

type IntervalTree[K Ordered[K], V any] struct {
	MinFn func(V) K
	MaxFn func(V) K
	inner RBTree[intervalKey[K], intervalValue[K, V]]
}

func (t *IntervalTree[K, V]) keyFn(v intervalValue[K, V]) intervalKey[K] {
	return intervalKey[K]{
		Min: t.MinFn(v.Val),
		Max: t.MaxFn(v.Val),
	}
}

func (t *IntervalTree[K, V]) attrFn(node *RBNode[intervalValue[K, V]]) {
	max := t.MaxFn(node.Value.Val)
	if node.Left != nil && node.Left.Value.SpanOfChildren.Max.Cmp(max) > 0 {
		max = node.Left.Value.SpanOfChildren.Max
	}
	if node.Right != nil && node.Right.Value.SpanOfChildren.Max.Cmp(max) > 0 {
		max = node.Right.Value.SpanOfChildren.Max
	}
	node.Value.SpanOfChildren.Max = max

	min := t.MinFn(node.Value.Val)
	if node.Left != nil && node.Left.Value.SpanOfChildren.Min.Cmp(min) < 0 {
		min = node.Left.Value.SpanOfChildren.Min
	}
	if node.Right != nil && node.Right.Value.SpanOfChildren.Min.Cmp(min) < 0 {
		min = node.Right.Value.SpanOfChildren.Min
	}
	node.Value.SpanOfChildren.Min = min
}

func (t *IntervalTree[K, V]) init() {
	if t.inner.KeyFn == nil {
		t.inner.KeyFn = t.keyFn
		t.inner.AttrFn = t.attrFn
	}
}

func (t *IntervalTree[K, V]) Delete(min, max K) {
	t.init()
	t.inner.Delete(intervalKey[K]{
		Min: min,
		Max: max,
	})
}

func (t *IntervalTree[K, V]) Equal(u *IntervalTree[K, V]) bool {
	return t.inner.Equal(&u.inner)
}

func (t *IntervalTree[K, V]) Insert(val V) {
	t.init()
	t.inner.Insert(intervalValue[K, V]{Val: val})
}

func (t *IntervalTree[K, V]) Min() (K, bool) {
	if t.inner.root == nil {
		var zero K
		return zero, false
	}
	return t.inner.root.Value.SpanOfChildren.Min, true
}

func (t *IntervalTree[K, V]) Max() (K, bool) {
	if t.inner.root == nil {
		var zero K
		return zero, false
	}
	return t.inner.root.Value.SpanOfChildren.Max, true
}

func (t *IntervalTree[K, V]) Lookup(k K) (V, bool) {
	return t.Search(k.Cmp)
}

func (t *IntervalTree[K, V]) Search(fn func(K) int) (V, bool) {
	node := t.inner.root
	for node != nil {
		switch {
		case t.keyFn(node.Value).ContainsFn(fn):
			return node.Value.Val, true
		case node.Left != nil && node.Left.Value.SpanOfChildren.ContainsFn(fn):
			node = node.Left
		case node.Right != nil && node.Right.Value.SpanOfChildren.ContainsFn(fn):
			node = node.Right
		default:
			node = nil
		}
	}
	var zero V
	return zero, false
}

func (t *IntervalTree[K, V]) searchAll(fn func(K) int, node *RBNode[intervalValue[K, V]], ret *[]V) {
	if node == nil {
		return
	}
	if !node.Value.SpanOfChildren.ContainsFn(fn) {
		return
	}
	t.searchAll(fn, node.Left, ret)
	if t.keyFn(node.Value).ContainsFn(fn) {
		*ret = append(*ret, node.Value.Val)
	}
	t.searchAll(fn, node.Right, ret)
}

func (t *IntervalTree[K, V]) SearchAll(fn func(K) int) []V {
	var ret []V
	t.searchAll(fn, t.inner.root, &ret)
	return ret
}

//func (t *IntervalTree[K, V]) Walk(fn func(*RBNode[V]) error) error
