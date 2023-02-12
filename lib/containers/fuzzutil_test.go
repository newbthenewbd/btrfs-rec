// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func SaveFuzz(f *testing.F, dat []byte) {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "go test fuzz v1\n[]byte(%q)\n", dat)
	sum := sha256.Sum256(buf.Bytes())
	filename := filepath.Join(
		"testdata",
		"fuzz",
		f.Name(),
		hex.EncodeToString(sum[:]))
	if err := os.WriteFile(filename, buf.Bytes(), 0o644); err != nil {
		f.Error(err)
	}
}
