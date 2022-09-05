// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"
)

func (t *RBTree[K, V]) ASCIIArt() string {
	var out strings.Builder
	t.root.asciiArt(&out, "", "", "")
	return out.String()
}

func (node *RBNode[V]) String() string {
	switch {
	case node == nil:
		return "nil"
	case node.Color == Red:
		return fmt.Sprintf("R(%v)", node.Value)
	default:
		return fmt.Sprintf("B(%v)", node.Value)
	}
}

func (node *RBNode[V]) asciiArt(w io.Writer, u, m, l string) {
	if node == nil {
		fmt.Fprintf(w, "%snil\n", m)
		return
	}

	node.Right.asciiArt(w, u+"     ", u+"  ,--", u+"  |  ")

	if node.Color == Red {
		fmt.Fprintf(w, "%s%v\n", m, node)
	} else {
		fmt.Fprintf(w, "%s%v\n", m, node)
	}

	node.Left.asciiArt(w, l+"  |  ", l+"  `--", l+"     ")
}

func checkRBTree[K constraints.Ordered, V any](t *testing.T, expectedSet Set[K], tree *RBTree[NativeOrdered[K], V]) {
	// 1. Every node is either red or black

	// 2. The root is black.
	require.Equal(t, Black, tree.root.getColor())

	// 3. Every nil is black.

	// 4. If a node is red, then both its children are black.
	require.NoError(t, tree.Walk(func(node *RBNode[V]) error {
		if node.getColor() == Red {
			require.Equal(t, Black, node.Left.getColor())
			require.Equal(t, Black, node.Right.getColor())
		}
		return nil
	}))

	// 5. For each node, all simple paths from the node to
	//    descendent leaves contain the same number of black
	//    nodes.
	var walkCnt func(node *RBNode[V], cnt int, leafFn func(int))
	walkCnt = func(node *RBNode[V], cnt int, leafFn func(int)) {
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
	require.NoError(t, tree.Walk(func(node *RBNode[V]) error {
		var cnts []int
		walkCnt(node, 0, func(cnt int) {
			cnts = append(cnts, cnt)
		})
		for i := range cnts {
			if cnts[0] != cnts[i] {
				require.Truef(t, false, "node %v: not all leafs have same black-count: %v", node.Value, cnts)
				break
			}
		}
		return nil
	}))

	// expected contents
	expectedOrder := make([]K, 0, len(expectedSet))
	for k := range expectedSet {
		expectedOrder = append(expectedOrder, k)
		node := tree.Lookup(NativeOrdered[K]{Val: k})
		require.NotNil(t, tree, node)
		require.Equal(t, k, tree.KeyFn(node.Value).Val)
	}
	sort.Slice(expectedOrder, func(i, j int) bool {
		return expectedOrder[i] < expectedOrder[j]
	})
	actOrder := make([]K, 0, len(expectedSet))
	require.NoError(t, tree.Walk(func(node *RBNode[V]) error {
		actOrder = append(actOrder, tree.KeyFn(node.Value).Val)
		return nil
	}))
	require.Equal(t, expectedOrder, actOrder)
}

func FuzzRBTree(f *testing.F) {
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
		tree := &RBTree[NativeOrdered[uint8], uint8]{
			KeyFn: func(x uint8) NativeOrdered[uint8] {
				return NativeOrdered[uint8]{Val: x}
			},
		}
		set := make(Set[uint8])
		checkRBTree(t, set, tree)
		t.Logf("\n%s\n", tree.ASCIIArt())
		for _, b := range dat {
			ins := (b & 0b0100_0000) != 0
			val := (b & 0b0011_1111)
			if ins {
				t.Logf("Insert(%v)", val)
				tree.Insert(val)
				set.Insert(val)
				t.Logf("\n%s\n", tree.ASCIIArt())
				node := tree.Lookup(NativeOrdered[uint8]{Val: val})
				require.NotNil(t, node)
				require.Equal(t, val, node.Value)
			} else {
				t.Logf("Delete(%v)", val)
				tree.Delete(NativeOrdered[uint8]{Val: val})
				delete(set, val)
				t.Logf("\n%s\n", tree.ASCIIArt())
				require.Nil(t, tree.Lookup(NativeOrdered[uint8]{Val: val}))
			}
			checkRBTree(t, set, tree)
		}
	})
}
