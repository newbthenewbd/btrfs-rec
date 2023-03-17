// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package profile

import (
	"io"
	"os"

	"github.com/datawire/dlib/derror"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
)

type flagSet struct {
	shutdown []StopFunc
}

func (fs *flagSet) Stop() error {
	var errs derror.MultiError
	for _, fn := range fs.shutdown {
		if err := fn(); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}

type flagValue struct {
	parent *flagSet
	start  startFunc
	curVal string
}

var _ pflag.Value = (*flagValue)(nil)

// String implements pflag.Value.
func (fv *flagValue) String() string { return fv.curVal }

// Set implements pflag.Value.
func (fv *flagValue) Set(filename string) error {
	if filename == "" {
		return nil
	}
	w, err := os.Create(filename)
	if err != nil {
		return err
	}
	shutdown, err := fv.start(w)
	if err != nil {
		return err
	}
	fv.curVal = filename
	fv.parent.shutdown = append(fv.parent.shutdown, func() error {
		err1 := shutdown()
		err2 := w.Close()
		if err1 != nil {
			return err1
		}
		return err2
	})
	return nil
}

// Type implements pflag.Value.
func (*flagValue) Type() string { return "filename" }

func pStart(name string) startFunc {
	return func(w io.Writer) (StopFunc, error) {
		return Profile(w, name)
	}
}

// AddProfileFlags adds flags to a pflag.FlagSet to write any (or all)
// of the standard profiles to a file, and returns a "stop" function
// to be called at program shutdown.
func AddProfileFlags(flags *pflag.FlagSet, prefix string) StopFunc {
	var root flagSet

	flags.Var(&flagValue{parent: &root, start: CPU}, prefix+"cpu", "Write a CPU profile to the file `cpu.pprof`")
	_ = cobra.MarkFlagFilename(flags, prefix+"cpu")

	flags.Var(&flagValue{parent: &root, start: Trace}, prefix+"trace", "Write a trace (https://pkg.go.dev/runtime/trace) to the file `trace.out`")
	_ = cobra.MarkFlagFilename(flags, prefix+"trace")

	flags.Var(&flagValue{parent: &root, start: pStart("goroutine")}, prefix+"goroutine", "Write a goroutine profile to the file `goroutine.pprof`")
	_ = cobra.MarkFlagFilename(flags, prefix+"goroutine")

	flags.Var(&flagValue{parent: &root, start: pStart("threadcreate")}, prefix+"threadcreate", "Write a threadcreate profile to the file `threadcreate.pprof`")
	_ = cobra.MarkFlagFilename(flags, prefix+"threadcreate")

	flags.Var(&flagValue{parent: &root, start: pStart("heap")}, prefix+"heap", "Write a heap profile to the file `heap.pprof`")
	_ = cobra.MarkFlagFilename(flags, prefix+"heap")

	flags.Var(&flagValue{parent: &root, start: pStart("allocs")}, prefix+"allocs", "Write an allocs profile to the file `allocs.pprof`")
	_ = cobra.MarkFlagFilename(flags, prefix+"allocs")

	flags.Var(&flagValue{parent: &root, start: pStart("block")}, prefix+"block", "Write a block profile to the file `block.pprof`")
	_ = cobra.MarkFlagFilename(flags, prefix+"block")

	flags.Var(&flagValue{parent: &root, start: pStart("mutex")}, prefix+"mutex", "Write a mutex profile to the file `mutex.pprof`")
	_ = cobra.MarkFlagFilename(flags, prefix+"mutex")

	return root.Stop
}
