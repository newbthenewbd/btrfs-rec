// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package textui

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

type LiveMemUse struct {
	mu    sync.Mutex
	stats runtime.MemStats
	last  time.Time
}

var _ fmt.Stringer = (*LiveMemUse)(nil)

var LiveMemUseUpdateInterval = Tunable(1 * time.Second)

func (o *LiveMemUse) String() string {
	o.mu.Lock()

	// runtime.ReadMemStats() calls stopTheWorld(), so we want to
	// rate-limit how often we call it.
	if now := time.Now(); now.Sub(o.last) > LiveMemUseUpdateInterval {
		runtime.ReadMemStats(&o.stats)
		o.last = now
	}

	// runtime.MemStats only knows about memory managed by the Go runtime;
	// even for a pure Go program, there's also
	//
	//   - memory mapped to the executable itself
	//   - vDSO and friends
	//
	// But those are pretty small, just a few MiB.
	//
	// OK, so: memory managed by the Go runtime.  runtime.MemStats is pretty
	// obtuse, I think it was designed more for "debugging the Go runtime"
	// than "monitoring the behavior of a Go program".  From the Go
	// runtime's perspective, regions of the virtual address space are in
	// one of 4 states (see `runtime/mem.go`):
	//
	//  - None : not mapped
	//
	//  - Reserved : mapped, but without r/w permissions (PROT_NONE); so
	//    this region isn't actually backed by anything
	//
	//  - Prepared : mapped, but allowed to be collected by the OS
	//    (MADV_FREE, or MADV_DONTNEED on systems without MADV_FREE); so
	//    this region may or may not actually be backed by anything.
	//
	//  - Ready : mapped, ready to be used
	//
	// Normal tools count Reserved+Prepared+Ready toward the VSS (which is a
	// little silly, when inspecting /proc/{pid}/maps to calculate the VSS,
	// IMO they should exclude maps without r/w permissions, which would
	// exclude Reserved), but we all know that VSS numbers are over
	// inflated.  And RSS only useful if we fit in RAM and don't spill to
	// swap (this is being written for btrfs-rec, which is quite likely to
	// consume all RAM on a laptop).  Useful numbers are Ready and Prepared;
	// as I said above, outside tools reporting Ready+Prepared would be easy
	// and useful, but none do; but I don't think outside tools have a way
	// to distinguish between Ready and Prepared (unless you can detect
	// MADV_FREE/MADV_DONTNEED in /proc/{pid}/smaps?).
	//
	// Of the 3 mapped states, here's how we get them from runtime:
	//
	//  - Reserved : AFAICT, you can't :(
	//
	//  - Prepared : `runtime.MemStats.HeapReleased`
	//
	//  - Ready : `runtime.MemStats.Sys - runtime.MemStats.HeapReleased`
	//    (that is, runtime.MemStats.Sys is Prepared+Ready)
	//
	// It's a bummer that we can't get Reserved from runtime, but as I've
	// said, it's not super useful; it's only use would really be
	// cross-referencing runtime's numbers against the VSS.
	//
	// The godocs for runtime.MemStats.Sys say "It's likely that not all of
	// the virtual address space is backed by physical memory at any given
	// moment, though in general it all was at some point."  That's both
	// confusing and a lie.  It's confusing because it doesn't give you
	// hooks to find out more; it could have said that this is
	// Ready+Prepared and that Prepared is the portion of the space that
	// might not be backed by physical memory, but instead it wants you to
	// throw your hands up and say "this is too weird for me to understand".
	// It's a lie because "in general it all was at some point" implies that
	// all Prepared memory was previously Ready, which is false; it can go
	// None->Reserved->Prepared (but it only does that Reserved->Prepared
	// transition if it thinks it will need to transition it to Ready very
	// soon, so maybe the doc author though that was negligible?).
	//
	// Now, those are still pretty opaque numbers; most of runtime.MemStats
	// goes toward accounting for what's going on inside of Ready space.
	//
	// For our purposes, we don't care too much about specifics of how Ready
	// space is being used; just how much is "actually storing data", vs
	// "overhead from heap-fragmentation", vs "idle".

	var (
		// We're going to add up all of the `o.stats.{thing}Sys`
		// variables and check that against `o.stats.Sys`, in order to
		// make sure that we're not missing any {thing} when adding up
		// `inuse`.
		calcSys = o.stats.HeapSys + o.stats.StackSys + o.stats.MSpanSys + o.stats.MCacheSys + o.stats.BuckHashSys + o.stats.GCSys + o.stats.OtherSys
		inuse   = o.stats.HeapInuse + o.stats.StackInuse + o.stats.MSpanInuse + o.stats.MCacheInuse + o.stats.BuckHashSys + o.stats.GCSys + o.stats.OtherSys
	)
	if calcSys != o.stats.Sys {
		panic("should not happen")
	}
	prepared := o.stats.HeapReleased
	ready := o.stats.Sys - prepared

	readyFragOverhead := o.stats.HeapInuse - o.stats.HeapAlloc
	readyData := inuse - readyFragOverhead
	readyIdle := ready - inuse

	o.mu.Unlock()

	return Sprintf("Ready+Prepared=%.1f (Ready=%.1f (data:%.1f + fragOverhead:%.1f + idle:%.1f) ; Prepared=%.1f)",
		IEC(ready+prepared, "B"),
		IEC(ready, "B"),
		IEC(readyData, "B"),
		IEC(readyFragOverhead, "B"),
		IEC(readyIdle, "B"),
		IEC(prepared, "B"))
}
