package rbtree

import (
	"lukeshu.com/btrfs-tools/pkg/util"
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
