// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui

import (
	"github.com/datawire/dlib/dlog"
	"github.com/sirupsen/logrus"
	"github.com/spf13/pflag"
)

type LogLevelFlag struct {
	logrus.Level
}

var _ pflag.Value = (*LogLevelFlag)(nil)

// Type implements pflag.Value.
func (lvl *LogLevelFlag) Type() string { return "loglevel" }

// Type implements pflag.Value.
func (lvl *LogLevelFlag) Set(str string) error {
	var err error
	lvl.Level, err = logrus.ParseLevel(str)
	return err
}

func NewLogger(lvl logrus.Level) dlog.Logger {
	logger := logrus.New()
	logger.SetLevel(lvl)
	return dlog.WrapLogrus(logger)
}
