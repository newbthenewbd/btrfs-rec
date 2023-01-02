// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"context"
	"io"
	"os"
	"time"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type runeScanner struct {
	progress       textui.Portion[int64]
	progressWriter *textui.Progress[textui.Portion[int64]]
	unreadCnt      uint64
	reader         *bufio.Reader
	closer         io.Closer
}

func newRuneScanner(ctx context.Context, fh *os.File) (*runeScanner, error) {
	fi, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	ret := &runeScanner{
		progress: textui.Portion[int64]{
			D: fi.Size(),
		},
		progressWriter: textui.NewProgress[textui.Portion[int64]](ctx, dlog.LogLevelInfo, 1*time.Second),
		reader:         bufio.NewReader(fh),
		closer:         fh,
	}
	return ret, nil
}

func (rs *runeScanner) ReadRune() (r rune, size int, err error) {
	r, size, err = rs.reader.ReadRune()
	if rs.unreadCnt > 0 {
		rs.unreadCnt--
	} else {
		rs.progress.N += int64(size)
		rs.progressWriter.Set(rs.progress)
	}
	return
}

func (rs *runeScanner) UnreadRune() error {
	rs.unreadCnt++
	return rs.reader.UnreadRune()
}

func (rs *runeScanner) Close() error {
	rs.progressWriter.Done()
	return rs.closer.Close()
}

func readJSONFile[T any](ctx context.Context, filename string) (T, error) {
	fh, err := os.Open(filename)
	if err != nil {
		var zero T
		return zero, err
	}
	buf, err := newRuneScanner(dlog.WithField(ctx, "btrfs.read-json-file", filename), fh)
	if err != nil {
		var zero T
		return zero, err
	}
	var ret T
	if err := lowmemjson.DecodeThenEOF(buf, &ret); err != nil {
		var zero T
		return zero, err
	}
	_ = buf.Close()
	return ret, nil
}

func writeJSONFile(w io.Writer, obj any, cfg lowmemjson.ReEncoder) (err error) {
	buffer := bufio.NewWriter(w)
	defer func() {
		if _err := buffer.Flush(); err == nil && _err != nil {
			err = _err
		}
	}()
	cfg.Out = buffer
	return lowmemjson.Encode(&cfg, obj)
}
