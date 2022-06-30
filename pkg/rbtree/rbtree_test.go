package rbtree

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func checkTree[K constraints.Ordered, V any](t *testing.T, tree *Tree[K, V]) {
	// 1. Every node is either red or black

	// 2. The root is black.
	assert.Equal(t, Black, tree.root.getColor())

	// 3. Every nil is black.

	// 4. If a node is red, then both its children are black.
	tree.Walk(func(node *Node[V]) {
		if node.getColor() == Red {
			assert.Equal(t, Black, node.Left.getColor())
			assert.Equal(t, Black, node.Right.getColor())
		}
	})

	// 5. For each node, all simple paths from the node to
	//    descendent leaves contain the same number of black
	//    nodes.
	var walkCnt func(node *Node[V], cnt int, leafFn func(int))
	walkCnt = func(node *Node[V], cnt int, leafFn func(int)) {
		if node.getColor() == Black {
			cnt++
		}
		if node == nil {
			leafFn(cnt)
			return
		}
		walkCnt(node.Left, cnt, leafFn)
		walkCnt(node.Right, cnt, leafFn)
	}
	tree.Walk(func(node *Node[V]) {
		var cnts []int
		walkCnt(node, 0, func(cnt int) {
			cnts = append(cnts, cnt)
		})
		for i := range cnts {
			if cnts[0] != cnts[i] {
				assert.Truef(t, false, "node %v: not all leafs have same black-count: %v", node.Value, cnts)
				break
			}
		}
	})
}

func FuzzTree(f *testing.F) {
	Ins := uint8(0b0100_0000)
	Del := uint8(0)

	f.Add([]uint8{})
	f.Add([]uint8{Ins | 5, Del | 5})
	f.Add([]uint8{Ins | 5, Del | 6})
	f.Add([]uint8{Del | 6})

	f.Add([]uint8{ // CLRS Figure 14.4
		Ins | 1,
		Ins | 2,
		Ins | 5,
		Ins | 7,
		Ins | 8,
		Ins | 11,
		Ins | 14,
		Ins | 15,

		Ins | 4,
	})

	f.Fuzz(func(t *testing.T, dat []uint8) {
		tree := &Tree[uint8, uint8]{
			KeyFn: func(x uint8) uint8 { return x },
		}
		checkTree(t, tree)
		for _, b := range dat {
			ins := (b & 0b0100_0000) != 0
			val := (b & 0b0011_1111)
			if ins {
				t.Logf("Insert(%v)", val)
				tree.Insert(val)
				node := tree.Lookup(val)
				require.NotNil(t, node)
				assert.Equal(t, val, node.Value)
			} else {
				t.Logf("Delete(%v)", val)
				if val == 25 {
					t.Logf("before:\n\n%s\n", tree.ASCIIArt())
				}
				tree.Delete(val)
				if val == 25 {
					t.Logf("after:\n\n%s\n", tree.ASCIIArt())
				}
				assert.Nil(t, tree.Lookup(val))
			}
			checkTree(t, tree)
		}
	})
}
