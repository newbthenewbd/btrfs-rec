package btrfsvol_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
)

func TestAddrFormat(t *testing.T) {
	t.Parallel()
	type TestCase struct {
		InputAddr btrfsvol.LogicalAddr
		InputFmt  string
		Output    string
	}
	addr := btrfsvol.LogicalAddr(0x3a41678000)
	testcases := map[string]TestCase{
		"v":   TestCase{InputAddr: addr, InputFmt: "%v", Output: "0x0000003a41678000"},
		"s":   TestCase{InputAddr: addr, InputFmt: "%s", Output: "0x0000003a41678000"},
		"q":   TestCase{InputAddr: addr, InputFmt: "%q", Output: `"0x0000003a41678000"`},
		"x":   TestCase{InputAddr: addr, InputFmt: "%x", Output: "3a41678000"},
		"d":   TestCase{InputAddr: addr, InputFmt: "%d", Output: "250205405184"},
		"neg": TestCase{InputAddr: -1, InputFmt: "%v", Output: "-0x000000000000001"},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			actual := fmt.Sprintf(tc.InputFmt, tc.InputAddr)
			assert.Equal(t, tc.Output, actual)
		})
	}
}
