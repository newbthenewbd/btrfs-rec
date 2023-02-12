// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"fmt"
	"reflect"
)

type Color bool

const (
	Black Color = false
	Red   Color = true
)

type RBNode[T Ordered[T]] struct {
	Parent, Left, Right *RBNode[T]

	Color Color

	Value T
}

func (node *RBNode[T]) getColor() Color {
	if node == nil {
		return Black
	}
	return node.Color
}

type RBTree[T Ordered[T]] struct {
	AttrFn func(*RBNode[T])
	root   *RBNode[T]
	len    int
}

func (t *RBTree[T]) Len() int {
	return t.len
}

func (t *RBTree[T]) Range(fn func(*RBNode[T]) bool) {
	t.root._range(fn)
}

func (node *RBNode[T]) _range(fn func(*RBNode[T]) bool) bool {
	if node == nil {
		return true
	}
	if !node.Left._range(fn) {
		return false
	}
	if !fn(node) {
		return false
	}
	if !node.Right._range(fn) {
		return false
	}
	return true
}

// Search the tree for a value that satisfied the given callbackk
// function.  A return value of 0 means to return this value; <0 means
// to go left on the tree (the value is too high), >0 means to go
// right on th etree (the value is too low).
//
//	        +-----+
//	        | v=8 | == 0 : this is it
//	        +-----+
//	       /       \
//	      /         \
//	<0 : go left   >0 : go right
//	    /             \
//	 +---+          +---+
//	 | 7 |          | 9 |
//	 +---+          +---+
//
// Returns nil if no such value is found.
func (t *RBTree[T]) Search(fn func(T) int) *RBNode[T] {
	ret, _ := t.root.search(fn)
	return ret
}

func (node *RBNode[T]) search(fn func(T) int) (exact, nearest *RBNode[T]) {
	var prev *RBNode[T]
	for {
		if node == nil {
			return nil, prev
		}
		direction := fn(node.Value)
		prev = node
		switch {
		case direction < 0:
			node = node.Left
		case direction == 0:
			return node, nil
		case direction > 0:
			node = node.Right
		}
	}
}

// Min returns the minimum value stored in the tree, or nil if the
// tree is empty.
func (t *RBTree[T]) Min() *RBNode[T] {
	return t.root.min()
}

func (node *RBNode[T]) min() *RBNode[T] {
	if node == nil {
		return nil
	}
	for {
		if node.Left == nil {
			return node
		}
		node = node.Left
	}
}

// Max returns the maximum value stored in the tree, or nil if the
// tree is empty.
func (t *RBTree[T]) Max() *RBNode[T] {
	return t.root.max()
}

func (node *RBNode[T]) max() *RBNode[T] {
	if node == nil {
		return nil
	}
	for {
		if node.Right == nil {
			return node
		}
		node = node.Right
	}
}

func (cur *RBNode[T]) Next() *RBNode[T] {
	if cur.Right != nil {
		return cur.Right.min()
	}
	child, parent := cur, cur.Parent
	for parent != nil && child == parent.Right {
		child, parent = parent, parent.Parent
	}
	return parent
}

func (cur *RBNode[T]) Prev() *RBNode[T] {
	if cur.Left != nil {
		return cur.Left.max()
	}
	child, parent := cur, cur.Parent
	for parent != nil && child == parent.Left {
		child, parent = parent, parent.Parent
	}
	return parent
}

// Subrange is like Search, but for when there may be more than one
// result.
func (t *RBTree[T]) Subrange(rangeFn func(T) int, handleFn func(*RBNode[T]) bool) {
	// Find the left-most acceptable node.
	_, node := t.root.search(func(v T) int {
		if rangeFn(v) <= 0 {
			return -1
		} else {
			return 1
		}
	})
	for node != nil && rangeFn(node.Value) > 0 {
		node = node.Next()
	}
	// Now walk forward until we hit the end.
	for node != nil && rangeFn(node.Value) == 0 {
		if keepGoing := handleFn(node); !keepGoing {
			return
		}
		node = node.Next()
	}
}

func (t *RBTree[T]) Equal(u *RBTree[T]) bool {
	if (t == nil) != (u == nil) {
		return false
	}
	if t == nil {
		return true
	}
	if t.len != u.len {
		return false
	}

	tSlice := make([]T, 0, t.len)
	t.Range(func(node *RBNode[T]) bool {
		tSlice = append(tSlice, node.Value)
		return true
	})

	uSlice := make([]T, 0, u.len)
	u.Range(func(node *RBNode[T]) bool {
		uSlice = append(uSlice, node.Value)
		return true
	})

	return reflect.DeepEqual(tSlice, uSlice)
}

func (t *RBTree[T]) parentChild(node *RBNode[T]) **RBNode[T] {
	switch {
	case node.Parent == nil:
		return &t.root
	case node.Parent.Left == node:
		return &node.Parent.Left
	case node.Parent.Right == node:
		return &node.Parent.Right
	default:
		panic(fmt.Errorf("node %p is not a child of its parent %p", node, node.Parent))
	}
}

func (t *RBTree[T]) updateAttr(node *RBNode[T]) {
	if t.AttrFn == nil {
		return
	}
	for node != nil {
		t.AttrFn(node)
		node = node.Parent
	}
}

func (t *RBTree[T]) leftRotate(x *RBNode[T]) {
	//        p                        p
	//        |                        |
	//      +---+                    +---+
	//      | x |                    | y |
	//      +---+                    +---+
	//     /     \         =>       /     \
	//    a    +---+              +---+    c
	//         | y |              | x |
	//         +---+              +---+
	//        /     \            /     \
	//       b       c          a       b

	// Define 'p', 'x', 'y', and 'b' per the above diagram.
	p := x.Parent
	pChild := t.parentChild(x)
	y := x.Right
	b := y.Left

	// Move things around

	y.Parent = p
	*pChild = y

	x.Parent = y
	y.Left = x

	if b != nil {
		b.Parent = x
	}
	x.Right = b

	t.updateAttr(x)
}

func (t *RBTree[T]) rightRotate(y *RBNode[T]) {
	//nolint:dupword
	//
	//           |                |
	//         +---+            +---+
	//         | y |            | x |
	//         +---+            +---+
	//        /     \    =>    /     \
	//      +---+    c        a    +---+
	//      | x |                  | y |
	//      +---+                  +---+
	//     /     \                /     \
	//    a       b              b       c

	// Define 'p', 'x', 'y', and 'b' per the above diagram.
	p := y.Parent
	pChild := t.parentChild(y)
	x := y.Left
	b := x.Right

	// Move things around

	x.Parent = p
	*pChild = x

	y.Parent = x
	x.Right = y

	if b != nil {
		b.Parent = y
	}
	y.Left = b

	t.updateAttr(y)
}

func (t *RBTree[T]) Insert(val T) {
	// Naive-insert

	exact, parent := t.root.search(val.Compare)
	if exact != nil {
		exact.Value = val
		return
	}
	t.len++

	node := &RBNode[T]{
		Color:  Red,
		Parent: parent,
		Value:  val,
	}
	switch {
	case parent == nil:
		t.root = node
	case val.Compare(parent.Value) < 0:
		parent.Left = node
	default:
		parent.Right = node
	}
	t.updateAttr(node)

	// Re-balance
	//
	// This is closely based on the algorithm presented in CLRS
	// 3e.

	for node.Parent.getColor() == Red {
		if node.Parent == node.Parent.Parent.Left {
			uncle := node.Parent.Parent.Right
			if uncle.getColor() == Red {
				node.Parent.Color = Black
				uncle.Color = Black
				node.Parent.Parent.Color = Red
				node = node.Parent.Parent
			} else {
				if node == node.Parent.Right {
					node = node.Parent
					t.leftRotate(node)
				}
				node.Parent.Color = Black
				node.Parent.Parent.Color = Red
				t.rightRotate(node.Parent.Parent)
			}
		} else {
			uncle := node.Parent.Parent.Left
			if uncle.getColor() == Red {
				node.Parent.Color = Black
				uncle.Color = Black
				node.Parent.Parent.Color = Red
				node = node.Parent.Parent
			} else {
				if node == node.Parent.Left {
					node = node.Parent
					t.rightRotate(node)
				}
				node.Parent.Color = Black
				node.Parent.Parent.Color = Red
				t.leftRotate(node.Parent.Parent)
			}
		}
	}
	t.root.Color = Black
}

func (t *RBTree[T]) transplant(oldNode, newNode *RBNode[T]) {
	*t.parentChild(oldNode) = newNode
	if newNode != nil {
		newNode.Parent = oldNode.Parent
	}
}

func (t *RBTree[T]) Delete(nodeToDelete *RBNode[T]) {
	if nodeToDelete == nil {
		return
	}
	t.len--

	// This is closely based on the algorithm presented in CLRS
	// 3e.

	// phase 1

	var nodeToRebalance *RBNode[T]
	var nodeToRebalanceParent *RBNode[T] // in case 'nodeToRebalance' is nil, which it can be
	needsRebalance := nodeToDelete.Color == Black

	switch {
	case nodeToDelete.Left == nil:
		nodeToRebalance = nodeToDelete.Right
		nodeToRebalanceParent = nodeToDelete.Parent
		t.transplant(nodeToDelete, nodeToDelete.Right)
	case nodeToDelete.Right == nil:
		nodeToRebalance = nodeToDelete.Left
		nodeToRebalanceParent = nodeToDelete.Parent
		t.transplant(nodeToDelete, nodeToDelete.Left)
	default:
		// The node being deleted has a child on both sides,
		// so we've go to reshuffle the parents a bit to make
		// room for those children.
		next := nodeToDelete.Next()
		if next.Parent == nodeToDelete {
			//         p                  p
			//         |                  |
			//      +-----+            +-----+
			//      | ntd |            | nxt |
			//      +-----+            +-----+
			//      /     \       =>   /     \
			//     a     +-----+      a      b
			//           | nxt |
			//           +-----+
			//            /   \
			//          nil   b
			nodeToRebalance = next.Right
			nodeToRebalanceParent = next

			*t.parentChild(nodeToDelete) = next
			next.Parent = nodeToDelete.Parent

			next.Left = nodeToDelete.Left
			next.Left.Parent = next
		} else {
			//         p                 p
			//         |                 |
			//      +-----+           +-----+
			//      | ntd |           | nxt |
			//      +-----+           +-----+
			//      /     \           /     \
			//     a       x         a       x
			//            / \    =>         / \
			//           y   z             y   z
			//          / \               / \
			//    +-----+  c             b   c
			//    | nxt |
			//    +-----+
			//    /     \
			//  nil     b
			y := next.Parent
			b := next.Right
			nodeToRebalance = b
			nodeToRebalanceParent = y

			*t.parentChild(nodeToDelete) = next
			next.Parent = nodeToDelete.Parent

			next.Left = nodeToDelete.Left
			next.Left.Parent = next

			next.Right = nodeToDelete.Right
			next.Right.Parent = next

			y.Left = b
			if b != nil {
				b.Parent = y
			}
		}

		// idk
		needsRebalance = next.Color == Black
		next.Color = nodeToDelete.Color
	}
	t.updateAttr(nodeToRebalanceParent)

	// phase 2

	if needsRebalance {
		node := nodeToRebalance
		nodeParent := nodeToRebalanceParent
		for node != t.root && node.getColor() == Black {
			if node == nodeParent.Left {
				sibling := nodeParent.Right
				if sibling.getColor() == Red {
					sibling.Color = Black
					nodeParent.Color = Red
					t.leftRotate(nodeParent)
					sibling = nodeParent.Right
				}
				if sibling.Left.getColor() == Black && sibling.Right.getColor() == Black {
					sibling.Color = Red
					node, nodeParent = nodeParent, nodeParent.Parent
				} else {
					if sibling.Right.getColor() == Black {
						sibling.Left.Color = Black
						sibling.Color = Red
						t.rightRotate(sibling)
						sibling = nodeParent.Right
					}
					sibling.Color = nodeParent.Color
					nodeParent.Color = Black
					sibling.Right.Color = Black
					t.leftRotate(nodeParent)
					node, nodeParent = t.root, nil
				}
			} else {
				sibling := nodeParent.Left
				if sibling.getColor() == Red {
					sibling.Color = Black
					nodeParent.Color = Red
					t.rightRotate(nodeParent)
					sibling = nodeParent.Left
				}
				if sibling.Right.getColor() == Black && sibling.Left.getColor() == Black {
					sibling.Color = Red
					node, nodeParent = nodeParent, nodeParent.Parent
				} else {
					if sibling.Left.getColor() == Black {
						sibling.Right.Color = Black
						sibling.Color = Red
						t.leftRotate(sibling)
						sibling = nodeParent.Left
					}
					sibling.Color = nodeParent.Color
					nodeParent.Color = Black
					sibling.Left.Color = Black
					t.rightRotate(nodeParent)
					node, nodeParent = t.root, nil
				}
			}
		}
		if node != nil {
			node.Color = Black
		}
	}
}
