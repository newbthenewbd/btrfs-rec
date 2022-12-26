// Copyright (C) 2019-2022  Ambassador Labs
// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: Apache-2.0
//
// Contains code based on:
// https://github.com/datawire/dlib/blob/b09ab2e017e16d261f05fff5b3b860d645e774d4/dlog/logger_logrus.go
// https://github.com/datawire/dlib/blob/b09ab2e017e16d261f05fff5b3b860d645e774d4/dlog/logger_testing.go
// https://github.com/telepresenceio/telepresence/blob/ece94a40b00a90722af36b12e40f91cbecc0550c/pkg/log/formatter.go

package textui

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/datawire/dlib/dlog"
	"github.com/spf13/pflag"
)

type LogLevelFlag struct {
	Level dlog.LogLevel
}

var _ pflag.Value = (*LogLevelFlag)(nil)

// Type implements pflag.Value.
func (lvl *LogLevelFlag) Type() string { return "loglevel" }

// Type implements pflag.Value.
func (lvl *LogLevelFlag) Set(str string) error {
	switch strings.ToLower(str) {
	case "error":
		lvl.Level = dlog.LogLevelError
	case "warn", "warning":
		lvl.Level = dlog.LogLevelWarn
	case "info":
		lvl.Level = dlog.LogLevelInfo
	case "debug":
		lvl.Level = dlog.LogLevelDebug
	case "trace":
		lvl.Level = dlog.LogLevelTrace
	default:
		return fmt.Errorf("invalid log level: %q", str)
	}
	return nil
}

// Type implements pflag.Value.
func (lvl *LogLevelFlag) String() string {
	switch lvl.Level {
	case dlog.LogLevelError:
		return "error"
	case dlog.LogLevelWarn:
		return "warn"
	case dlog.LogLevelInfo:
		return "info"
	case dlog.LogLevelDebug:
		return "debug"
	case dlog.LogLevelTrace:
		return "trace"
	default:
		panic(fmt.Errorf("invalid log level: %#v", lvl.Level))
	}
}

type logger struct {
	parent *logger
	out    io.Writer
	lvl    dlog.LogLevel

	// only valid if parent is non-nil
	fieldKey string
	fieldVal any
}

var _ dlog.OptimizedLogger = (*logger)(nil)

func NewLogger(out io.Writer, lvl dlog.LogLevel) dlog.Logger {
	return &logger{
		out: out,
		lvl: lvl,
	}
}

// Helper implements dlog.Logger.
func (l *logger) Helper() {}

// WithField implements dlog.Logger.
func (l *logger) WithField(key string, value any) dlog.Logger {
	return &logger{
		parent: l,
		out:    l.out,
		lvl:    l.lvl,

		fieldKey: key,
		fieldVal: value,
	}
}

type logWriter struct {
	log *logger
	lvl dlog.LogLevel
}

// Write implements io.Writer.
func (lw logWriter) Write(data []byte) (int, error) {
	lw.log.log(lw.lvl, func(w io.Writer) {
		_, _ = w.Write(data)
	})
	return len(data), nil
}

// StdLogger implements dlog.Logger.
func (l *logger) StdLogger(lvl dlog.LogLevel) *log.Logger {
	return log.New(logWriter{log: l, lvl: lvl}, "", 0)
}

// Log implements dlog.Logger.
func (l *logger) Log(lvl dlog.LogLevel, msg string) {
	panic("should not happen: optimized log methods should be used instead")
}

// UnformattedLog implements dlog.OptimizedLogger.
func (l *logger) UnformattedLog(lvl dlog.LogLevel, args ...any) {
	l.log(lvl, func(w io.Writer) {
		printer.Fprint(w, args...)
	})
}

// UnformattedLogln implements dlog.OptimizedLogger.
func (l *logger) UnformattedLogln(lvl dlog.LogLevel, args ...any) {
	l.log(lvl, func(w io.Writer) {
		printer.Fprintln(w, args...)
	})
}

// UnformattedLogf implements dlog.OptimizedLogger.
func (l *logger) UnformattedLogf(lvl dlog.LogLevel, format string, args ...any) {
	l.log(lvl, func(w io.Writer) {
		printer.Fprintf(w, format, args...)
	})
}

var (
	logBuf     bytes.Buffer
	logMu      sync.Mutex
	thisModDir string
)

func init() {
	_, file, _, _ := runtime.Caller(0)
	thisModDir = filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

func (l *logger) log(lvl dlog.LogLevel, writeMsg func(io.Writer)) {
	// boilerplate /////////////////////////////////////////////////////////
	if lvl > l.lvl {
		return
	}
	// This is optimized for mostly-single-threaded usage.  If I cared more
	// about multi-threaded performance, I'd trade in some
	// memory-use/allocations and (1) instead of using a static `logBuf`,
	// I'd have a `logBufPool` `sync.Pool`, and (2) have the the final call
	// to `l.out.Write()` be the only thing protected by `logMu`.
	logMu.Lock()
	defer logMu.Unlock()
	defer logBuf.Reset()

	// time ////////////////////////////////////////////////////////////////
	now := time.Now()
	const timeFmt = "2006-01-02 15:04:05.0000"
	logBuf.WriteString(timeFmt)
	now.AppendFormat(logBuf.Bytes()[:0], timeFmt)

	// level ///////////////////////////////////////////////////////////////
	switch lvl {
	case dlog.LogLevelError:
		logBuf.WriteString(" ERR")
	case dlog.LogLevelWarn:
		logBuf.WriteString(" WRN")
	case dlog.LogLevelInfo:
		logBuf.WriteString(" INF")
	case dlog.LogLevelDebug:
		logBuf.WriteString(" DBG")
	case dlog.LogLevelTrace:
		logBuf.WriteString(" TRC")
	}

	// fields (early) //////////////////////////////////////////////////////
	fields := make(map[string]any)
	var fieldKeys []string
	for f := l; f.parent != nil; f = f.parent {
		if _, exists := fields[f.fieldKey]; exists {
			continue
		}
		fields[f.fieldKey] = f.fieldVal
		fieldKeys = append(fieldKeys, f.fieldKey)
	}
	sort.Slice(fieldKeys, func(i, j int) bool {
		iOrd := fieldOrd(fieldKeys[i])
		jOrd := fieldOrd(fieldKeys[j])
		if iOrd != jOrd {
			return iOrd < jOrd
		}
		return fieldKeys[i] < fieldKeys[j]
	})
	nextField := len(fieldKeys)
	for i, fieldKey := range fieldKeys {
		if fieldOrd(fieldKey) >= 0 {
			nextField = i
			break
		}
		printer.Fprintf(&logBuf, " %s=%v", fieldName(fieldKey), fields[fieldKey])
	}

	// message /////////////////////////////////////////////////////////////
	logBuf.WriteString(" : ")
	writeMsg(&logBuf)

	// fields (late) ///////////////////////////////////////////////////////
	if nextField < len(fieldKeys) {
		logBuf.WriteString(" :")
	}
	for _, fieldKey := range fieldKeys[nextField:] {
		printer.Fprintf(&logBuf, " %s=%v", fieldName(fieldKey), fields[fieldKey])
	}

	// caller //////////////////////////////////////////////////////////////
	const (
		thisModule             = "git.lukeshu.com/btrfs-progs-ng"
		thisPackage            = "git.lukeshu.com/btrfs-progs-ng/lib/textui"
		maximumCallerDepth int = 25
		minimumCallerDepth int = 3 // runtime.Callers + .log + .Log
	)
	var pcs [maximumCallerDepth]uintptr
	depth := runtime.Callers(minimumCallerDepth, pcs[:])
	frames := runtime.CallersFrames(pcs[:depth])
	for f, again := frames.Next(); again; f, again = frames.Next() {
		if !strings.HasPrefix(f.Function, thisModule+"/") {
			continue
		}
		if strings.HasPrefix(f.Function, thisPackage+".") {
			continue
		}
		if nextField == len(fieldKeys) {
			logBuf.WriteString(" :")
		}
		file := f.File[strings.LastIndex(f.File, thisModDir+"/")+len(thisModDir+"/"):]
		fmt.Fprintf(&logBuf, " (from %s:%d)", file, f.Line)
		break
	}

	// boilerplate /////////////////////////////////////////////////////////
	logBuf.WriteByte('\n')
	_, _ = l.out.Write(logBuf.Bytes())
}

// fieldOrd returns the sort-position for a given log-field-key.  Lower return
// values should be positioned on the left when logging, and higher values
// should be positioned on the right; values <0 should be on the left of the log
// message, while values ≥0 should be on the right of the log message.
func fieldOrd(key string) int {
	switch key {
	case "THREAD": // dgroup
		return -7
	case "dexec.pid":
		return -6
	case "dexec.stream":
		return -5
	case "dexec.data":
		return -4
	case "dexec.err":
		return -3

	case "btrfsinspect.scandevices.dev":
		return -1
	case "btrfsinspect.rebuild-mappings.step":
		return -2
	case "btrfsinspect.rebuild-mappings.substep":
		return -1

	default:
		return 1
	}
}

func fieldName(key string) string {
	switch key {
	case "THREAD":
		return "thread"
	default:
		switch {
		case strings.HasPrefix(key, "btrfsinspect.scandevices."):
			return strings.TrimPrefix(key, "btrfsinspect.scandevices.")
		case strings.HasPrefix(key, "btrfsinspect.rebuild-mappings."):
			return strings.TrimPrefix(key, "btrfsinspect.rebuild-mappings.")
		default:
			return key
		}
	}
}
