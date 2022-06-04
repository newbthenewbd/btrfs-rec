package util

import (
	"fmt"
	"strings"
)

func BitfieldString[T ~uint8 | ~uint16 | ~uint32 | ~uint64](bitfield T, bitnames []string) string {
	var out strings.Builder
	fmt.Fprintf(&out, "0x%0x", uint64(bitfield))
	if bitfield == 0 {
		out.WriteString("(none)")
	} else {
		rest := bitfield
		sep := '('
		for i := 0; rest != 0; i++ {
			if rest&(1<<i) != 0 {
				out.WriteRune(sep)
				if i < len(bitnames) {
					out.WriteString(bitnames[i])
				} else {
					fmt.Fprintf(&out, "(1<<%d)", i)
				}
				sep = '|'
			}
			rest &^= 1 << i
		}
		out.WriteRune(')')
	}
	return out.String()
}
