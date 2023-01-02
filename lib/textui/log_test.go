// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui_test

import (
	"context"
	"strings"
	"testing"

	"github.com/datawire/dlib/dlog"
	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func logLineRegexp(inner string) string {
	return `[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{4} ` + inner + ` \(from lib/textui/log_test\.go:[0-9]+\)\n`
}

func TestLogFormat(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	ctx := dlog.WithLogger(context.Background(), textui.NewLogger(&out, dlog.LogLevelTrace))
	dlog.Debugf(ctx, "foo %d", 12345)
	assert.Regexp(t,
		`^`+logLineRegexp(`DBG : foo 12,345 :`)+`$`,
		out.String())
}

func TestLogLevel(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	ctx := dlog.WithLogger(context.Background(), textui.NewLogger(&out, dlog.LogLevelInfo))
	dlog.Error(ctx, "Error")
	dlog.Warn(ctx, "Warn")
	dlog.Info(ctx, "Info")
	dlog.Debug(ctx, "Debug")
	dlog.Trace(ctx, "Trace")
	dlog.Trace(ctx, "Trace")
	dlog.Debug(ctx, "Debug")
	dlog.Info(ctx, "Info")
	dlog.Warn(ctx, "Warn")
	dlog.Error(ctx, "Error")
	assert.Regexp(t,
		`^`+
			logLineRegexp(`ERR : Error :`)+
			logLineRegexp(`WRN : Warn :`)+
			logLineRegexp(`INF : Info :`)+
			logLineRegexp(`INF : Info :`)+
			logLineRegexp(`WRN : Warn :`)+
			logLineRegexp(`ERR : Error :`)+
			`$`,
		out.String())
}

func TestLogField(t *testing.T) {
	t.Parallel()
	var out strings.Builder
	ctx := dlog.WithLogger(context.Background(), textui.NewLogger(&out, dlog.LogLevelInfo))
	ctx = dlog.WithField(ctx, "foo", 12345)
	dlog.Info(ctx, "msg")
	assert.Regexp(t,
		`^`+logLineRegexp(`INF : msg : foo=12,345`)+`$`,
		out.String())
}
