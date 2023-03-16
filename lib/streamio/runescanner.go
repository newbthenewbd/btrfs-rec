// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package streamio implements utilities for working with streaming
// I/O.
package streamio

import (
	"bufio"
	"context"
	"io"
	"os"
	"time"

	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type RuneScanner interface {
	io.RuneScanner
	io.Closer
}

type runeScanner struct {
	ctx            context.Context //nolint:containedctx // For detecting shutdown from methods
	done           <-chan struct{}
	progress       textui.Portion[int64]
	progressWriter *textui.Progress[textui.Portion[int64]]
	unreadCnt      uint64
	reader         *bufio.Reader
	closer         io.Closer
}

// NewRuneScanner returns an io.RuneScanner (and io.Closer) that
// bufferes a file, similar to bufio.NewReader.  There are two
// advantages over bufio.NewReader:
//
//   - It takes a Context, and causes reads to fail once the Context is
//     canceled; allowing large parse operations to be gracefully cut
//     short.
//
//   - It logs the progress of reading the file via textui.Progress.
func NewRuneScanner(ctx context.Context, fh *os.File) (RuneScanner, error) {
	fi, err := fh.Stat()
	if err != nil {
		return nil, err
	}
	ret := &runeScanner{
		ctx:  ctx,
		done: ctx.Done(),
		progress: textui.Portion[int64]{
			D: fi.Size(),
		},
		progressWriter: textui.NewProgress[textui.Portion[int64]](ctx, dlog.LogLevelInfo, textui.Tunable(1*time.Second)),
		reader:         bufio.NewReader(fh),
		closer:         fh,
	}
	return ret, nil
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

//nolint:gomnd // False positive: gomnd.ignored-functions=[textui.Tunable] doesn't support type params.
var runeThrottle = textui.Tunable[int64](64)

// ReadRune implements io.RuneReader.
func (rs *runeScanner) ReadRune() (r rune, size int, err error) {
	// According to the profiler, checking if the rs.ctx.Done()
	// channel is closed is faster than checking if rs.ctx.Err()
	// is non-nil.
	if rs.unreadCnt == 0 && isClosed(rs.done) {
		return 0, 0, rs.ctx.Err()
	}
	r, size, err = rs.reader.ReadRune()
	if rs.unreadCnt > 0 {
		rs.unreadCnt--
	} else {
		rs.progress.N += int64(size)
		if rs.progress.D < runeThrottle || rs.progress.N%runeThrottle == 0 || rs.progress.N > rs.progress.D-runeThrottle {
			rs.progressWriter.Set(rs.progress)
		}
	}
	return
}

// ReadRune implements io.RuneScanner.
func (rs *runeScanner) UnreadRune() error {
	if err := rs.reader.UnreadRune(); err != nil {
		return err
	}
	rs.unreadCnt++
	return nil
}

// ReadRune implements io.Closer.
func (rs *runeScanner) Close() error {
	rs.progressWriter.Done()
	return rs.closer.Close()
}
