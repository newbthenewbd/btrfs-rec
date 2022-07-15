// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsvol

import (
	"fmt"
	"sort"

	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

// logical => []physical
type chunkMapping struct {
	LAddr      LogicalAddr
	PAddrs     []QualifiedPhysicalAddr
	Size       AddrDelta
	SizeLocked bool
	Flags      containers.Optional[BlockGroupFlags]
}

type ChunkMapping = chunkMapping

// return -1 if 'a' is wholly to the left of 'b'
// return 0 if there is some overlap between 'a' and 'b'
// return 1 if 'a is wholly to the right of 'b'
func (a chunkMapping) cmpRange(b chunkMapping) int {
	switch {
	case a.LAddr.Add(a.Size) <= b.LAddr:
		// 'a' is wholly to the left of 'b'.
		return -1
	case b.LAddr.Add(b.Size) <= a.LAddr:
		// 'a' is wholly to the right of 'b'.
		return 1
	default:
		// There is some overlap.
		return 0
	}
}

func (a chunkMapping) union(rest ...chunkMapping) (chunkMapping, error) {
	// sanity check
	for _, chunk := range rest {
		if a.cmpRange(chunk) != 0 {
			return chunkMapping{}, fmt.Errorf("chunks don't overlap")
		}
	}
	chunks := append([]chunkMapping{a}, rest...)
	// figure out the logical range (.LAddr and .Size)
	beg := chunks[0].LAddr
	end := chunks[0].LAddr.Add(chunks[0].Size)
	for _, chunk := range chunks {
		beg = slices.Min(beg, chunk.LAddr)
		end = slices.Max(end, chunk.LAddr.Add(chunk.Size))
	}
	ret := chunkMapping{
		LAddr: beg,
		Size:  end.Sub(beg),
	}
	for _, chunk := range chunks {
		if chunk.SizeLocked {
			ret.SizeLocked = true
			if ret.Size != chunk.Size {
				return chunkMapping{}, fmt.Errorf("member chunk has locked size=%v, but union would have size=%v",
					chunk.Size, ret.Size)
			}
		}
	}
	// figure out the physical stripes (.PAddrs)
	paddrs := make(map[QualifiedPhysicalAddr]struct{})
	for _, chunk := range chunks {
		offsetWithinRet := chunk.LAddr.Sub(ret.LAddr)
		for _, stripe := range chunk.PAddrs {
			paddrs[stripe.Add(-offsetWithinRet)] = struct{}{}
		}
	}
	ret.PAddrs = maps.Keys(paddrs)
	sort.Slice(ret.PAddrs, func(i, j int) bool {
		return ret.PAddrs[i].Cmp(ret.PAddrs[j]) < 0
	})
	// figure out the flags (.Flags)
	for _, chunk := range chunks {
		if !chunk.Flags.OK {
			continue
		}
		if !ret.Flags.OK {
			ret.Flags = chunk.Flags
		}
		if ret.Flags != chunk.Flags {
			return ret, fmt.Errorf("mismatch flags: %v != %v", ret.Flags.Val, chunk.Flags.Val)
		}
	}
	// done
	return ret, nil
}
