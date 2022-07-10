package rbtree

import (
	"reflect"

	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

// SearchRange is like Search, but returns all nodes that match the
// function; assuming that they are contiguous.
func (t *Tree[K, V]) SearchRange(fn func(V) int) []V {
	middle := t.Search(fn)
	if middle == nil {
		return nil
	}
	ret := []V{middle.Value}
	for node := t.Prev(middle); node != nil && fn(node.Value) == 0; node = t.Prev(node) {
		ret = append(ret, node.Value)
	}
	util.ReverseSlice(ret)
	for node := t.Next(middle); node != nil && fn(node.Value) == 0; node = t.Next(node) {
		ret = append(ret, node.Value)
	}
	return ret
}

func (t *Tree[K, V]) Equal(u *Tree[K, V]) bool {
	if (t == nil) != (u == nil) {
		return false
	}
	if t == nil {
		return true
	}

	var tSlice []V
	_ = t.Walk(func(node *Node[V]) error {
		tSlice = append(tSlice, node.Value)
		return nil
	})

	var uSlice []V
	_ = u.Walk(func(node *Node[V]) error {
		uSlice = append(uSlice, node.Value)
		return nil
	})

	return reflect.DeepEqual(tSlice, uSlice)
}
