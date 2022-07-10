// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rbtree

import (
	"fmt"
	"io"
	"strings"
)

func (t *Tree[K, V]) ASCIIArt() string {
	var out strings.Builder
	t.root.asciiArt(&out, "", "", "")
	return out.String()
}

func (node *Node[V]) String() string {
	switch {
	case node == nil:
		return "nil"
	case node.Color == Red:
		return fmt.Sprintf("R(%v)", node.Value)
	default:
		return fmt.Sprintf("B(%v)", node.Value)
	}
}

func (node *Node[V]) asciiArt(w io.Writer, u, m, l string) {
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
