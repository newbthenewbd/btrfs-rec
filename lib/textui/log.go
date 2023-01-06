// Copyright (C) 2019-2022  Ambassador Labs
// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
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
	"unicode"

	"github.com/datawire/dlib/dlog"
	"github.com/spf13/pflag"

	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
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
		_, _ = printer.Fprint(w, args...)
	})
}

// UnformattedLogln implements dlog.OptimizedLogger.
func (l *logger) UnformattedLogln(lvl dlog.LogLevel, args ...any) {
	l.log(lvl, func(w io.Writer) {
		_, _ = printer.Fprintln(w, args...)
	})
}

// UnformattedLogf implements dlog.OptimizedLogger.
func (l *logger) UnformattedLogf(lvl dlog.LogLevel, format string, args ...any) {
	l.log(lvl, func(w io.Writer) {
		_, _ = printer.Fprintf(w, format, args...)
	})
}

var (
	logBufPool = containers.SyncPool[*bytes.Buffer]{
		New: func() *bytes.Buffer {
			return new(bytes.Buffer)
		},
	}
	logMu      sync.Mutex
	thisModDir string
)

func init() {
	//nolint:dogsled // I can't change the signature of the stdlib.
	_, file, _, _ := runtime.Caller(0)
	thisModDir = filepath.Dir(filepath.Dir(filepath.Dir(file)))
}

func (l *logger) log(lvl dlog.LogLevel, writeMsg func(io.Writer)) {
	// boilerplate /////////////////////////////////////////////////////////
	if lvl > l.lvl {
		return
	}
	logBuf, _ := logBufPool.Get()
	defer logBufPool.Put(logBuf)
	defer logBuf.Reset()

	// time ////////////////////////////////////////////////////////////////
	now := time.Now()
	const timeFmt = "15:04:05.0000"
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
		writeField(logBuf, fieldKey, fields[fieldKey])
	}

	// message /////////////////////////////////////////////////////////////
	logBuf.WriteString(" : ")
	writeMsg(logBuf)

	// fields (late) ///////////////////////////////////////////////////////
	if nextField < len(fieldKeys) {
		logBuf.WriteString(" :")
	}
	for _, fieldKey := range fieldKeys[nextField:] {
		writeField(logBuf, fieldKey, fields[fieldKey])
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
		fmt.Fprintf(logBuf, " (from %s:%d)", file, f.Line)
		break
	}

	// boilerplate /////////////////////////////////////////////////////////
	logBuf.WriteByte('\n')

	logMu.Lock()
	_, _ = l.out.Write(logBuf.Bytes())
	logMu.Unlock()
}

// fieldOrd returns the sort-position for a given log-field-key.  Lower return
// values should be positioned on the left when logging, and higher values
// should be positioned on the right; values <0 should be on the left of the log
// message, while values ≥0 should be on the right of the log message.
func fieldOrd(key string) int {
	switch key {
	// dlib ////////////////////////////////////////////////////////////////
	case "THREAD": // dgroup
		return -99
	case "dexec.pid":
		return -98
	case "dexec.stream":
		return -97
	case "dexec.data":
		return -96
	case "dexec.err":
		return -95

	// btrfsinspect scandevices ////////////////////////////////////////////
	case "btrfsinspect.scandevices.dev":
		return -1

	// btrfsinspect rebuild-mappings ///////////////////////////////////////
	case "btrfsinspect.rebuild-mappings.step":
		return -2
	case "btrfsinspect.rebuild-mappings.substep":
		return -1

	// btrfsinspect rebuild-nodes //////////////////////////////////////////
	case "btrfsinspect.rebuild-nodes.step":
		return -50
	// step=read-fs-data
	case "btrfsinspect.rebuild-nodes.read.substep":
		return -1
	// step=rebuild
	case "btrfsinspect.rebuild-nodes.rebuild.pass":
		return -49
	case "btrfsinspect.rebuild-nodes.rebuild.substep":
		return -48
	case "btrfsinspect.rebuild-nodes.rebuild.substep.progress":
		return -47
	// step=rebuild, substep=collect-items (1/3)
	// step=rebuild, substep=process-items (2/3)
	case "btrfsinspect.rebuild-nodes.rebuild.process.item":
		return -25
	// step=rebuild, substep=apply-augments (3/3)
	case "btrfsinspect.rebuild-nodes.rebuild.augment.tree":
		return -25
	// step=rebuild (any substep)
	case "btrfsinspect.rebuild-nodes.rebuild.want.key":
		return -9
	case "btrfsinspect.rebuild-nodes.rebuild.want.reason":
		return -8
	case "btrfsinspect.rebuild-nodes.rebuild.add-tree":
		return -7
	case "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.key":
		return -6
	case "btrfsinspect.rebuild-nodes.rebuild.add-tree.want.reason":
		return -5
	case "btrfsinspect.rebuild-nodes.rebuild.add-root":
		return -4
	case "btrfsinspect.rebuild-nodes.rebuild.index-inc-items":
		return -3
	case "btrfsinspect.rebuild-nodes.rebuild.index-exc-items":
		return -2
	case "btrfsinspect.rebuild-nodes.rebuild.index-nodes":
		return -1

	// other ///////////////////////////////////////////////////////////////
	case "btrfs.read-json-file":
		return -1
	default:
		return 1
	}
}

func writeField(w io.Writer, key string, val any) {
	valBuf, _ := logBufPool.Get()
	defer func() {
		// The wrapper `func()` is important to defer
		// evaluating `valBuf`, since we might re-assign it
		// below.
		valBuf.Reset()
		logBufPool.Put(valBuf)
	}()
	_, _ = printer.Fprint(valBuf, val)
	needsQuote := false
	if bytes.HasPrefix(valBuf.Bytes(), []byte(`"`)) {
		needsQuote = true
	} else {
		for _, r := range valBuf.Bytes() {
			if !(unicode.IsPrint(rune(r)) && r != ' ') {
				needsQuote = true
				break
			}
		}
	}
	if needsQuote {
		valBuf2, _ := logBufPool.Get()
		fmt.Fprintf(valBuf2, "%q", valBuf.Bytes())
		valBuf.Reset()
		logBufPool.Put(valBuf)
		valBuf = valBuf2
	}

	valStr := valBuf.Bytes()
	name := key

	switch {
	case name == "THREAD":
		name = "thread"
		switch {
		case len(valStr) == 0 || bytes.Equal(valStr, []byte("/main")):
			return
		default:
			if bytes.HasPrefix(valStr, []byte("/main/")) {
				valStr = valStr[len("/main/"):]
			} else if bytes.HasPrefix(valStr, []byte("/")) {
				valStr = valStr[len("/"):]
			}
		}
	case strings.HasSuffix(name, ".pass"):
		fmt.Fprintf(w, "/pass-%s", valStr)
		return
	case strings.HasSuffix(name, ".substep") && name != "btrfsinspect.rebuild-nodes.rebuild.add-tree.substep":
		fmt.Fprintf(w, "/%s", valStr)
		return
	case strings.HasPrefix(name, "btrfsinspect."):
		name = strings.TrimPrefix(name, "btrfsinspect.")
		switch {
		case strings.HasPrefix(name, "scandevices."):
			name = strings.TrimPrefix(name, "scandevices.")
		case strings.HasPrefix(name, "rebuild-mappings."):
			name = strings.TrimPrefix(name, "rebuild-mappings.")
		case strings.HasPrefix(name, "rebuild-nodes."):
			name = strings.TrimPrefix(name, "rebuild-nodes.")
			switch {
			case strings.HasPrefix(name, "read."):
				name = strings.TrimPrefix(name, "read.")
			case strings.HasPrefix(name, "rebuild."):
				name = strings.TrimPrefix(name, "rebuild.")
			}
		}
	case strings.HasPrefix(name, "btrfs."):
		name = strings.TrimPrefix(name, "btrfs.")
	}

	fmt.Fprintf(w, " %s=%s", name, valStr)
}
