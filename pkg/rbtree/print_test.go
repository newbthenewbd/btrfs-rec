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

func (node *Node[V]) asciiArt(w io.Writer, u, m, l string) {
	if node == nil {
		fmt.Fprintf(w, "%snil\n", m)
		return
	}

	node.Right.asciiArt(w, u+"     ", u+"  ,--", u+"  |  ")

	if node.Color == Red {
		fmt.Fprintf(w, "%sR(%v)\n", m, node.Value)
	} else {
		fmt.Fprintf(w, "%sB(%v)\n", m, node.Value)
	}

	node.Left.asciiArt(w, l+"  |  ", l+"  `--", l+"     ")
}
