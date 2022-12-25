// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsinspect

import (
	"io"
	"sort"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func PrintLogicalSpace(out io.Writer, fs *btrfs.FS) {
	mappings := fs.LV.Mappings()
	var prevBeg, prevEnd btrfsvol.LogicalAddr
	var sumHole, sumChunk btrfsvol.AddrDelta
	for _, mapping := range mappings {
		if mapping.LAddr > prevEnd {
			size := mapping.LAddr.Sub(prevEnd)
			textui.Fprintf(out, "logical_hole laddr=%v size=%v\n", prevEnd, size)
			sumHole += size
		}
		if mapping.LAddr != prevBeg {
			if !mapping.Flags.OK {
				textui.Fprintf(out, "chunk laddr=%v size=%v flags=(missing)\n",
					mapping.LAddr, mapping.Size)
			} else {
				textui.Fprintf(out, "chunk laddr=%v size=%v flags=%v\n",
					mapping.LAddr, mapping.Size, mapping.Flags.Val)
			}
		}
		textui.Fprintf(out, "\tstripe dev_id=%v paddr=%v\n",
			mapping.PAddr.Dev, mapping.PAddr.Addr)
		sumChunk += mapping.Size
		prevBeg = mapping.LAddr
		prevEnd = mapping.LAddr.Add(mapping.Size)
	}
	textui.Fprintf(out, "total logical holes      = %v (%d)\n", sumHole, int64(sumHole))
	textui.Fprintf(out, "total logical chunks     = %v (%d)\n", sumChunk, int64(sumChunk))
	textui.Fprintf(out, "total logical addr space = %v (%d)\n", prevEnd, int64(prevEnd))
}

func PrintPhysicalSpace(out io.Writer, fs *btrfs.FS) {
	mappings := fs.LV.Mappings()
	sort.Slice(mappings, func(i, j int) bool {
		return mappings[i].PAddr.Cmp(mappings[j].PAddr) < 0
	})

	var prevDev btrfsvol.DeviceID = 0
	var prevEnd btrfsvol.PhysicalAddr
	var sumHole, sumExt btrfsvol.AddrDelta
	for _, mapping := range mappings {
		if mapping.PAddr.Dev != prevDev {
			prevDev = mapping.PAddr.Dev
			prevEnd = 0
		}
		if mapping.PAddr.Addr > prevEnd {
			size := mapping.PAddr.Addr.Sub(prevEnd)
			textui.Fprintf(out, "physical_hole paddr=%v size=%v\n", prevEnd, size)
			sumHole += size
		}
		textui.Fprintf(out, "devext dev=%v paddr=%v size=%v laddr=%v\n",
			mapping.PAddr.Dev, mapping.PAddr.Addr, mapping.Size, mapping.LAddr)
		sumExt += mapping.Size
		prevEnd = mapping.PAddr.Addr.Add(mapping.Size)
	}
	textui.Fprintf(out, "total physical holes      = %v (%d)\n", sumHole, int64(sumHole))
	textui.Fprintf(out, "total physical extents    = %v (%d)\n", sumExt, int64(sumExt))
	textui.Fprintf(out, "total physical addr space = %v (%d)\n", prevEnd, int64(prevEnd))
}
