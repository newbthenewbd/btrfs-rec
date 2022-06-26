package internal

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/util"
)

type (
	PhysicalAddr int64
	LogicalAddr  int64
	AddrDelta    int64
)

func formatAddr(addr int64, f fmt.State, verb rune) {
	switch verb {
	case 'v', 's', 'q':
		str := fmt.Sprintf("%#016x", addr)
		fmt.Fprintf(f, util.FmtStateString(f, verb), str)
	default:
		fmt.Fprintf(f, util.FmtStateString(f, verb), addr)
	}
}

func (a PhysicalAddr) Format(f fmt.State, verb rune) { formatAddr(int64(a), f, verb) }
func (a LogicalAddr) Format(f fmt.State, verb rune)  { formatAddr(int64(a), f, verb) }
func (d AddrDelta) Format(f fmt.State, verb rune)    { formatAddr(int64(d), f, verb) }

func (a PhysicalAddr) Sub(b PhysicalAddr) AddrDelta { return AddrDelta(a - b) }
func (a LogicalAddr) Sub(b LogicalAddr) AddrDelta   { return AddrDelta(a - b) }

func (a PhysicalAddr) Add(b AddrDelta) PhysicalAddr { return a + PhysicalAddr(b) }
func (a LogicalAddr) Add(b AddrDelta) LogicalAddr   { return a + LogicalAddr(b) }
