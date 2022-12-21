// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/datawire/dlib/dlog"
)

type Stats interface {
	comparable
	fmt.Stringer
}

type Progress[T Stats] struct {
	ctx      context.Context
	lvl      dlog.LogLevel
	interval time.Duration

	cancel context.CancelFunc
	done   chan struct{}

	cur     atomic.Value // Value[T]
	oldStat T
	oldLine string
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

func (p *Progress[T]) Set(val T) {
	if p.cur.Swap(val) == nil {
		go p.run()
	}
}

func (p *Progress[T]) Done() {
	p.cancel()
	<-p.done
}

func (p *Progress[T]) flush(force bool) {
	cur := p.cur.Load().(T)
	if !force && cur == p.oldStat {
		return
	}
	defer func() { p.oldStat = cur }()

	line := cur.String()
	if !force && line == p.oldLine {
		return
	}
	defer func() { p.oldLine = line }()

	dlog.Log(p.ctx, p.lvl, line)
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
