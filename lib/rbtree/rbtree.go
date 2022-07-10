package rbtree

import (
	"fmt"

	"golang.org/x/exp/constraints"
)

type Color bool

const (
	Black = Color(false)
	Red   = Color(true)
)

type Node[V any] struct {
	Parent, Left, Right *Node[V]

	Color Color

	Value V
}

func (node *Node[V]) getColor() Color {
	if node == nil {
		return Black
	}
	return node.Color
}

type Tree[K constraints.Ordered, V any] struct {
	KeyFn func(V) K
	root  *Node[V]
}

func (t *Tree[K, V]) Walk(fn func(*Node[V]) error) error {
	return t.root.walk(fn)
}

func (node *Node[V]) walk(fn func(*Node[V]) error) error {
	if node == nil {
		return nil
	}
	if err := node.Left.walk(fn); err != nil {
		return err
	}
	if err := fn(node); err != nil {
		return err
	}
	if err := node.Right.walk(fn); err != nil {
		return err
	}
	return nil
}

// Search the tree for a value that satisfied the given callbackk
// function.  A return value of 0 means to to return this value; <0
// means to go left on the tree (the value is too high), >0 means to
// go right on th etree (the value is too low).
//
//             +-----+
//             | v=8 | == 0 : this is it
//             +-----+
//            /       \
//           /         \
//     <0 : go left   >0 : go right
//         /             \
//      +---+          +---+
//      | 7 |          | 9 |
//      +---+          +---+
//
// Returns nil if no such value is found.
//
// Search is good for advanced lookup, like when a range of values is
// acceptable.  For simple exact-value lookup, use Lookup.
func (t *Tree[K, V]) Search(fn func(V) int) *Node[V] {
	ret, _ := t.root.search(fn)
	return ret
}

func (node *Node[V]) search(fn func(V) int) (exact, nearest *Node[V]) {
	var prev *Node[V]
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

func (t *Tree[K, V]) exactKey(key K) func(V) int {
	return func(val V) int {
		valKey := t.KeyFn(val)
		switch {
		case key < valKey:
			return -1
		case key > valKey:
			return 1
		default: // key == valKey:
			return 0
		}
	}
}

// Lookup looks up the value for an exact key.  If no such value
// exists, nil is returned.
func (t *Tree[K, V]) Lookup(key K) *Node[V] {
	return t.Search(t.exactKey(key))
}

// Min returns the minimum value stored in the tree, or nil if the
// tree is empty.
func (t *Tree[K, V]) Min() *Node[V] {
	return t.root.min()
}

func (node *Node[V]) min() *Node[V] {
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
func (t *Tree[K, V]) Max() *Node[V] {
	return t.root.max()
}

func (node *Node[V]) max() *Node[V] {
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

func (t *Tree[K, V]) Next(cur *Node[V]) *Node[V] {
	return cur.next()
}

func (cur *Node[V]) next() *Node[V] {
	if cur.Right != nil {
		return cur.Right.min()
	}
	child, parent := cur, cur.Parent
	for parent != nil && child == parent.Right {
		child, parent = parent, parent.Parent
	}
	return parent
}

func (t *Tree[K, V]) Prev(cur *Node[V]) *Node[V] {
	return cur.prev()
}

func (cur *Node[V]) prev() *Node[V] {
	if cur.Left != nil {
		return cur.Left.max()
	}
	child, parent := cur, cur.Parent
	for parent != nil && child == parent.Left {
		child, parent = parent, parent.Parent
	}
	return parent
}

func (t *Tree[K, V]) parentChild(node *Node[V]) **Node[V] {
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

func (t *Tree[K, V]) leftRotate(x *Node[V]) {
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
}

func (t *Tree[K, V]) rightRotate(y *Node[V]) {
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
}

func (t *Tree[K, V]) Insert(val V) {
	// Naive-insert

	key := t.KeyFn(val)
	exact, parent := t.root.search(t.exactKey(key))
	if exact != nil {
		exact.Value = val
		return
	}

	node := &Node[V]{
		Color:  Red,
		Parent: parent,
		Value:  val,
	}
	if parent == nil {
		t.root = node
	} else if key < t.KeyFn(parent.Value) {
		parent.Left = node
	} else {
		parent.Right = node
	}

	// Re-balance

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

func (t *Tree[K, V]) transplant(old, new *Node[V]) {
	*t.parentChild(old) = new
	if new != nil {
		new.Parent = old.Parent
	}
}

func (t *Tree[K, V]) Delete(key K) {
	nodeToDelete := t.Lookup(key)
	if nodeToDelete == nil {
		return
	}

	var nodeToRebalance *Node[V]
	var nodeToRebalanceParent *Node[V] // in case 'nodeToRebalance' is nil, which it can be
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
		next := nodeToDelete.next()
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
