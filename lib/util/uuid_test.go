package util_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/util"
)

func TestParseUUID(t *testing.T) {
	t.Parallel()
	type TestCase struct {
		Input     string
		OutputVal util.UUID
		OutputErr string
	}
	testcases := map[string]TestCase{
		"basic":    {Input: "a0dd94ed-e60c-42e8-8632-64e8d4765a43", OutputVal: util.UUID{0xa0, 0xdd, 0x94, 0xed, 0xe6, 0x0c, 0x42, 0xe8, 0x86, 0x32, 0x64, 0xe8, 0xd4, 0x76, 0x5a, 0x43}},
		"too-long": {Input: "a0dd94ed-e60c-42e8-8632-64e8d4765a43a", OutputErr: `too long to be a UUID: "a0dd94ed-e60c-42e8-8632-64e8d4765a43"|"a"`},
		"bad char": {Input: "a0dd94ej-e60c-42e8-8632-64e8d4765a43a", OutputErr: `illegal byte in UUID: "a0dd94e"|"j"|"-e60c-42e8-8632-64e8d4765a43a"`},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			val, err := util.ParseUUID(tc.Input)
			assert.Equal(t, tc.OutputVal, val)
			if tc.OutputErr == "" {
				assert.NoError(t, err)
			} else {
				assert.EqualError(t, err, tc.OutputErr)
			}
		})
	}
}

func TestUUIDFormat(t *testing.T) {
	t.Parallel()
	type TestCase struct {
		InputUUID util.UUID
		InputFmt  string
		Output    string
	}
	uuid := util.MustParseUUID("a0dd94ed-e60c-42e8-8632-64e8d4765a43")
	testcases := map[string]TestCase{
		"s":     {InputUUID: uuid, InputFmt: "%s", Output: "a0dd94ed-e60c-42e8-8632-64e8d4765a43"},
		"x":     {InputUUID: uuid, InputFmt: "%x", Output: "a0dd94ede60c42e8863264e8d4765a43"},
		"X":     {InputUUID: uuid, InputFmt: "%X", Output: "A0DD94EDE60C42E8863264E8D4765A43"},
		"v":     {InputUUID: uuid, InputFmt: "%v", Output: "a0dd94ed-e60c-42e8-8632-64e8d4765a43"},
		"40s":   {InputUUID: uuid, InputFmt: "|% 40s", Output: "|    a0dd94ed-e60c-42e8-8632-64e8d4765a43"},
		"#115v": {InputUUID: uuid, InputFmt: "|%#115v", Output: "|           util.UUID{0xa0, 0xdd, 0x94, 0xed, 0xe6, 0xc, 0x42, 0xe8, 0x86, 0x32, 0x64, 0xe8, 0xd4, 0x76, 0x5a, 0x43}"},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()
			actual := fmt.Sprintf(tc.InputFmt, tc.InputUUID)
			assert.Equal(t, tc.Output, actual)
		})
	}
}
