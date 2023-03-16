// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Package profile implements a uniform interface for getting
// profiling information from the Go runtime.
package profile

import (
	"io"
	"runtime/pprof"
	"runtime/trace"
)

type StopFunc = func() error

type startFunc = func(io.Writer) (StopFunc, error)

// CPU arranges to write a CPU profile to the given Writer, and
// returns a function to be called on shutdown.
func CPU(w io.Writer) (StopFunc, error) {
	if err := pprof.StartCPUProfile(w); err != nil {
		return nil, err
	}
	return func() error {
		pprof.StopCPUProfile()
		return nil
	}, nil
}

var _ startFunc = CPU

// Profile arranges to write the given named-profile to the given
// Writer, and returns a function to be called on shutdown.
//
// CPU profiles are not named profiles; there is a separate .CPU()
// function for writing CPU profiles.
//
// The Go runtime has several built-in named profiles, and it is
// possible for programs to create their own named profiles with
// runtime/pprof.NewProfile().
//
// This package provides ProfileXXX constants for the built-in named
// profiles, and a .Profiles() function that return the list of all
// profile names.
func Profile(w io.Writer, name string) (StopFunc, error) {
	return func() error {
		if prof := pprof.Lookup(name); prof != nil {
			return prof.WriteTo(w, 0)
		}
		return nil
	}, nil
}

// Profiles returns a list of all profile names that may be passed to
// .Profile(); both profiles built-in to the Go runtime, and
// program-added profiles.
func Profiles() []string {
	full := pprof.Profiles()
	names := make([]string, len(full))
	for i, prof := range full {
		names[i] = prof.Name()
	}
	return names
}

// The Go runtime's built-in named profiles; to be passed to
// .Profile().
const (
	ProfileGoroutine    = "goroutine"
	ProfileThreadCreate = "threadcreate"
	ProfileHeap         = "heap"
	ProfileAllocs       = "allocs"
	ProfileBlock        = "block"
	ProfileMutex        = "mutex"
)

// Trace arranges to write a trace (https://pkg.go.dev/runtime/trace)
// to the given Writer, and returns a function to be called on
// shutdown.
func Trace(w io.Writer) (StopFunc, error) {
	if err := trace.Start(w); err != nil {
		return nil, err
	}
	return func() error {
		trace.Stop()
		return nil
	}, nil
}

var _ startFunc = Trace
