package rbtree

import (
	"lukeshu.com/btrfs-tools/pkg/util"
)

// SearchRange is like Search, but returns all nodes that match the
// function; assuming that they are contiguous.
func (t *Tree[K, V]) SearchRange(fn func(V) int) []*Node[V] {
	middle := t.Search(fn)
	if middle == nil {
		return nil
	}
	ret := []*Node[V]{middle}
	for node := t.Prev(middle); node != nil && fn(node.Value) == 0; node = t.Prev(node) {
		ret = append(ret, node)
	}
	util.ReverseSlice(ret)
	for node := t.Next(middle); node != nil && fn(node.Value) == 0; node = t.Next(node) {
		ret = append(ret, node)
	}
	return ret
}
