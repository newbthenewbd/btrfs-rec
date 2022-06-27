package btrfsvol

import (
	"fmt"
	"sort"

	"lukeshu.com/btrfs-tools/pkg/util"
)

// logical => []physical
type chunkMapping struct {
	LAddr  LogicalAddr
	PAddrs []QualifiedPhysicalAddr
	Size   AddrDelta
	Flags  *BlockGroupFlags
}

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
		beg = util.Min(beg, chunk.LAddr)
		end = util.Max(end, chunk.LAddr.Add(chunk.Size))
	}
	ret := chunkMapping{
		LAddr: beg,
		Size:  end.Sub(beg),
	}
	// figure out the physical stripes (.PAddrs)
	paddrs := make(map[QualifiedPhysicalAddr]struct{})
	for _, chunk := range chunks {
		offsetWithinRet := chunk.LAddr.Sub(ret.LAddr)
		for _, stripe := range chunk.PAddrs {
			paddrs[QualifiedPhysicalAddr{
				Dev:  stripe.Dev,
				Addr: stripe.Addr.Add(-offsetWithinRet),
			}] = struct{}{}
		}
	}
	ret.PAddrs = make([]QualifiedPhysicalAddr, 0, len(paddrs))
	for paddr := range paddrs {
		ret.PAddrs = append(ret.PAddrs, paddr)
	}
	sort.Slice(ret.PAddrs, func(i, j int) bool {
		return ret.PAddrs[i].Cmp(ret.PAddrs[j]) < 0
	})
	// figure out the flags (.Flags)
	for _, chunk := range chunks {
		if chunk.Flags == nil {
			continue
		}
		if ret.Flags == nil {
			val := *chunk.Flags
			ret.Flags = &val
		}
		if *ret.Flags != *chunk.Flags {
			return ret, fmt.Errorf("mismatch flags: %v != %v", *ret.Flags, *chunk.Flags)
		}
	}
	// done
	return ret, nil
}
