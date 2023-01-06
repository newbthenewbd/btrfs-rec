// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfssum_test

import (
	"bytes"
	"testing"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
)

func TestShortSumEncodeJSON(t *testing.T) {
	t.Parallel()
	type TestCase struct {
		InputSum   btrfssum.ShortSum
		OutputJSON string
	}
	testcases := map[string]TestCase{
		"short": {
			InputSum:   "xyz",
			OutputJSON: `"78797a"`,
		},
		"long": {
			InputSum:   "0123456789abcdefghijklmnopqrstuvwxyz;:.,ABCDEFG",
			OutputJSON: `["303132333435363738396162636465666768696a6b6c6d6e6f707172737475767778797a3b3a2e2c","41424344454647"]`,
		},
		"medium": { // exactly the maximum string length
			InputSum:   "0123456789abcdefghijklmnopqrstuvwxyz;:.,",
			OutputJSON: `"303132333435363738396162636465666768696a6b6c6d6e6f707172737475767778797a3b3a2e2c"`,
		},
	}
	for tcName, tc := range testcases {
		tc := tc
		t.Run(tcName, func(t *testing.T) {
			t.Parallel()

			var jsonBuf bytes.Buffer
			assert.NoError(t, lowmemjson.NewEncoder(&jsonBuf).Encode(tc.InputSum))
			assert.Equal(t, tc.OutputJSON, jsonBuf.String())

			var rtSum btrfssum.ShortSum
			assert.NoError(t, lowmemjson.NewDecoder(&jsonBuf).DecodeThenEOF(&rtSum))
			assert.Equal(t, tc.InputSum, rtSum)
		})
	}
}

func FuzzShortSumJSONFuzz(f *testing.F) {
	f.Fuzz(func(t *testing.T, _inSum []byte) {
		t.Logf("in = %q", _inSum)
		inSum := btrfssum.ShortSum(_inSum)

		var jsonBuf bytes.Buffer
		assert.NoError(t, lowmemjson.NewEncoder(&jsonBuf).Encode(inSum))
		t.Logf("json = %q", jsonBuf.Bytes())

		var outSum btrfssum.ShortSum
		assert.NoError(t, lowmemjson.NewDecoder(&jsonBuf).DecodeThenEOF(&outSum))
		assert.Equal(t, inSum, outSum)
	})
}
