// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildmappings

import (
	"errors"

	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
)

type kmpPattern[K ~int64 | ~int, V comparable] interface {
	PatLen() K
	// Get the value at 'pos' in the sequence.  Positions start at
	// 0 and increment naturally.  It is invalid to call Get(pos)
	// with a pos that is >= Len().  If there is a gap/wildcard at
	// pos, ok is false.
	PatGet(pos K) (v V, ok bool)
}

func kmpSelfEq[K ~int64 | ~int, V comparable](s kmpPattern[K, V], aI K, bI K) bool {
	aV, aOK := s.PatGet(aI)
	bV, bOK := s.PatGet(bI)
	if !aOK || !bOK {
		return true
	}
	return aV == bV
}

// buildKMPTable takes the string 'substr', and returns a table such
// that 'table[matchLen-1]' is the largest value 'val' for which 'val < matchLen' and
// 'substr[:val] == substr[matchLen-val:matchLen]'.
func buildKMPTable[K ~int64 | ~int, V comparable](substr kmpPattern[K, V]) []K {
	substrLen := substr.PatLen()

	table := make([]K, substrLen)
	for j := K(0); j < substrLen; j++ {
		if j == 0 {
			// First entry must always be 0 (in order to
			// satisfy 'val < matchLen').
			continue
		}
		val := table[j-1]
		// not a match; go back
		for val > 0 && !kmpSelfEq(substr, j, val) {
			val = table[val-1]
		}
		// is a match; go forward
		if kmpSelfEq(substr, val, j) {
			val++
		}
		table[j] = val
	}
	return table
}

func kmpEq[K ~int64 | ~int, V comparable](aV V, bS kmpPattern[K, V], bI K) bool {
	bV, ok := bS.PatGet(bI)
	if !ok {
		return true
	}
	return aV == bV
}

// indexAll returns the starting-position of all possibly-overlapping
// occurrences of 'substr' in the 'str' sequence.
//
// Will hop around in 'substr', but will only get the natural sequence
// [0...) in order from 'str'.
//
// Will panic if the length of 'substr' is 0.
//
// The 'substr' may include wildcard characters by returning
// ErrWildcard for a position.
//
// Uses the Knuth-Morris-Pratt algorithm.
func indexAll[K ~int64 | ~int, V comparable](str diskio.Sequence[K, V], substr kmpPattern[K, V]) []K {
	table := buildKMPTable(substr)
	substrLen := K(len(table))
	if substrLen == 0 {
		panic(errors.New("rebuildmappings.IndexAll: empty substring"))
	}

	var matches []K
	var curMatchBeg K
	var curMatchLen K

	strLen := str.SeqLen()
	for pos := K(0); pos < strLen; pos++ {
		chr := str.SeqGet(pos)

		// Consider 'chr'
		for curMatchLen > 0 && !kmpEq(chr, substr, curMatchLen) { // shorten the match
			overlap := table[curMatchLen-1]
			curMatchBeg += curMatchLen - overlap
			curMatchLen = overlap
		}
		if kmpEq(chr, substr, curMatchLen) { // lengthen the match
			if curMatchLen == 0 {
				curMatchBeg = pos
			}
			curMatchLen++
			if curMatchLen == substrLen {
				matches = append(matches, curMatchBeg)
				overlap := table[curMatchLen-1]
				curMatchBeg += curMatchLen - overlap
				curMatchLen = overlap
			}
		}
	}
	return matches
}
