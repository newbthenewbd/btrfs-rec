// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio_test

import (
	"bytes"
	"testing"
	"testing/iotest"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type byteReaderWithName struct {
	*bytes.Reader
	name string
}

func (r byteReaderWithName) Name() string {
	return r.name
}

func (byteReaderWithName) Close() error {
	return nil
}

func (byteReaderWithName) WriteAt([]byte, int64) (int, error) {
	panic("not implemented")
}

func FuzzStatefulReader(f *testing.F) {
	f.Fuzz(func(t *testing.T, content []byte) {
		t.Logf("content=%q", content)
		var file diskio.File[int64] = byteReaderWithName{
			Reader: bytes.NewReader(content),
			name:   t.Name(),
		}
		reader := diskio.NewStatefulFile[int64](file)
		if err := iotest.TestReader(reader, content); err != nil {
			t.Error(err)
		}
	})
}

func FuzzStatefulBufferedReader(f *testing.F) {
	f.Fuzz(func(t *testing.T, content []byte) {
		t.Logf("content=%q", content)
		var file diskio.File[int64] = byteReaderWithName{
			Reader: bytes.NewReader(content),
			name:   t.Name(),
		}
		ctx := dlog.NewTestContext(t, false)
		file = diskio.NewBufferedFile[int64](ctx, file, 4, 2)
		reader := diskio.NewStatefulFile[int64](file)
		if err := iotest.TestReader(reader, content); err != nil {
			t.Error(err)
		}
	})
}
