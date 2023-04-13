// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui

import (
	"context"
	"fmt"
	"time"

	"git.lukeshu.com/go/typedsync"
	"github.com/datawire/dlib/dlog"
)

type Stats interface {
	comparable
	fmt.Stringer
}

// Progress helps display to the user the ongoing progress of a long
// task.
//
// There are few usage requirements to watch out for:
//
//   - .Set() must have been called at least once before you call
//     .Done().  The easiest way to ensure this is to call .Set right
//     after creating the progress, or right before calling .Done().  I
//     advise against counting on a loop to have called .Set() at least
//     once.
type Progress[T Stats] struct {
	ctx      context.Context //nolint:containedctx // captured for separate goroutine
	lvl      dlog.LogLevel
	interval time.Duration

	cancel context.CancelFunc
	done   chan struct{}

	cur     typedsync.Value[T]
	oldStat T
	oldLine string

	// This isn't a functional part, but is useful for helping us
	// to detect misuse.
	last time.Time
}

func NewProgress[T Stats](ctx context.Context, lvl dlog.LogLevel, interval time.Duration) *Progress[T] {
	ctx, cancel := context.WithCancel(ctx)
	ret := &Progress[T]{
		ctx:      ctx,
		lvl:      lvl,
		interval: interval,

		cancel: cancel,
		done:   make(chan struct{}),
	}
	return ret
}

// Set update the Progress.  Rate-limiting prevents this from being
// expensive, or from spamming the user; it is reasonably safe to call
// .Set in a tight inner loop.
//
// It is safe to call Set concurrently.
func (p *Progress[T]) Set(val T) {
	if _, hadOld := p.cur.Swap(val); !hadOld {
		go p.run()
	}
}

// Done closes the Progress; it flushes out one last status update (if
// nescessary), and releases resources associated with the Progress.
//
// It is safe to call Done multiple times, or concurrently.
//
// It will panic if Done is called without having called Set at least
// once.
func (p *Progress[T]) Done() {
	p.cancel()
	if _, started := p.cur.Load(); !started {
		panic("textui.Progress: .Done called without ever calling .Set")
	}
	<-p.done
}

func (p *Progress[T]) flush(force bool) {
	// Check how long it's been since we last printed something.
	// If this grows too big, it probably means that either the
	// program deadlocked or that we forgot to call .Done().
	now := time.Now()
	if !p.last.IsZero() && now.Sub(p.last) > Tunable(2*time.Minute) {
		dlog.Error(p.ctx, "stale Progress")
		panic("stale Progress")
	}

	// Load the data to print.
	cur, ok := p.cur.Load()
	if !ok {
		panic("should not happen")
	}
	if !force && cur == p.oldStat {
		return
	}
	defer func() { p.oldStat = cur }()

	// Format the data as text.
	line := cur.String()
	if !force && line == p.oldLine {
		return
	}
	defer func() { p.oldLine = line }()

	// Print.
	dlog.Log(p.ctx, p.lvl, line)
	p.last = now
}

func (p *Progress[T]) run() {
	p.flush(true)
	ticker := time.NewTicker(p.interval)
	for {
		select {
		case <-p.ctx.Done():
			ticker.Stop()
			p.flush(false)
			close(p.done)
			return
		case <-ticker.C:
			p.flush(false)
		}
	}
}
