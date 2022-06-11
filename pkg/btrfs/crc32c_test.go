package btrfs_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
)

func TestCSumFormat(t *testing.T) {
	t.Parallel()
	type TestCase struct {
		InputSum btrfs.CSum
		InputFmt string
		Output   string
	}
	csum := btrfs.CSum{0xbd, 0x7b, 0x41, 0xf4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
	testcases := map[string]TestCase{
		"s":   TestCase{InputSum: csum, InputFmt: "%s", Output: "bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"x":   TestCase{InputSum: csum, InputFmt: "%x", Output: "bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"v":   TestCase{InputSum: csum, InputFmt: "%v", Output: "bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"70s": TestCase{InputSum: csum, InputFmt: "|% 70s", Output: "|      bd7b41f400000000000000000000000000000000000000000000000000000000"},
		"#180v":  TestCase{InputSum: csum, InputFmt: "%#180v", Output: "      btrfs.CSum{0xbd, 0x7b, 0x41, 0xf4, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}"},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			actual := fmt.Sprintf(tc.InputFmt, tc.InputSum)
			assert.Equal(t, tc.Output, actual)
		})
	}
}
