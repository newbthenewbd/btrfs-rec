// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/exp/constraints"

	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func (t *RBTree[T]) ASCIIArt() string {
	var out strings.Builder
	t.root.asciiArt(&out, "", "", "")
	return out.String()
}

func (node *RBNode[T]) String() string {
	switch {
	case node == nil:
		return "nil"
	case node.Color == Red:
		return fmt.Sprintf("R(%v)", node.Value)
	default:
		return fmt.Sprintf("B(%v)", node.Value)
	}
}

func (node *RBNode[T]) asciiArt(w io.Writer, u, m, l string) {
	if node == nil {
		fmt.Fprintf(w, "%snil\n", m)
		return
	}

	node.Right.asciiArt(w, u+"     ", u+"  ,--", u+"  |  ")
	fmt.Fprintf(w, "%s%v\n", m, node)
	node.Left.asciiArt(w, l+"  |  ", l+"  `--", l+"     ")
}

func checkRBTree[T constraints.Ordered](t *testing.T, expectedSet Set[T], tree *RBTree[NativeOrdered[T]]) {
	// 1. Every node is either red or black

	// 2. The root is black.
	require.Equal(t, Black, tree.root.getColor())

	// 3. Every nil is black.

	// 4. If a node is red, then both its children are black.
	tree.Range(func(node *RBNode[NativeOrdered[T]]) bool {
		if node.getColor() == Red {
			require.Equal(t, Black, node.Left.getColor())
			require.Equal(t, Black, node.Right.getColor())
		}
		return true
	})

	// 5. For each node, all simple paths from the node to
	//    descendent leaves contain the same number of black
	//    nodes.
	var walkCnt func(node *RBNode[NativeOrdered[T]], cnt int, leafFn func(int))
	walkCnt = func(node *RBNode[NativeOrdered[T]], cnt int, leafFn func(int)) {
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
	tree.Range(func(node *RBNode[NativeOrdered[T]]) bool {
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
		return true
	})

	// expected contents
	expectedOrder := make([]T, 0, len(expectedSet))
	for v := range expectedSet {
		expectedOrder = append(expectedOrder, v)
		node := tree.Search(NativeOrdered[T]{Val: v}.Compare)
		require.NotNil(t, tree, node)
	}
	slices.Sort(expectedOrder)
	actOrder := make([]T, 0, len(expectedSet))
	tree.Range(func(node *RBNode[NativeOrdered[T]]) bool {
		actOrder = append(actOrder, node.Value.Val)
		return true
	})
	require.Equal(t, expectedOrder, actOrder)
	require.Equal(t, len(expectedSet), tree.Len())
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
		tree := new(RBTree[NativeOrdered[uint8]])
		set := make(Set[uint8])
		checkRBTree(t, set, tree)
		t.Logf("\n%s\n", tree.ASCIIArt())
		for _, b := range dat {
			ins := (b & 0b0100_0000) != 0
			val := (b & 0b0011_1111)
			if ins {
				t.Logf("Insert(%v)", val)
				tree.Insert(NativeOrdered[uint8]{Val: val})
				set.Insert(val)
				t.Logf("\n%s\n", tree.ASCIIArt())
				node := tree.Search(NativeOrdered[uint8]{Val: val}.Compare)
				require.NotNil(t, node)
				require.Equal(t, val, node.Value.Val)
			} else {
				t.Logf("Delete(%v)", val)
				tree.Delete(tree.Search(NativeOrdered[uint8]{Val: val}.Compare))
				delete(set, val)
				t.Logf("\n%s\n", tree.ASCIIArt())
				require.Nil(t, tree.Search(NativeOrdered[uint8]{Val: val}.Compare))
			}
			checkRBTree(t, set, tree)
		}
	})
}
