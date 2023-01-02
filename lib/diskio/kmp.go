// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"errors"
	"io"
)

var ErrWildcard = errors.New("wildcard")

func kmpEq2[K ~int64, V comparable](aS Sequence[K, V], aI K, bS Sequence[K, V], bI K) bool {
	aV, aErr := aS.Get(aI)
	bV, bErr := bS.Get(bI)
	if aErr != nil {
		//nolint:errorlint // The == is just a fast-path; we still fall back to errors.Is.
		if aErr == ErrWildcard || errors.Is(aErr, ErrWildcard) {
			aV = bV
			aErr = nil
		} else {
			panic(aErr)
		}
	}
	if bErr != nil {
		//nolint:errorlint // The == is just a fast-path; we still fall back to errors.Is.
		if bErr == ErrWildcard || errors.Is(bErr, ErrWildcard) {
			bV = aV
			bErr = nil
		} else {
			panic(bErr)
		}
	}
	if aErr != nil || bErr != nil {
		return false
	}
	return aV == bV
}

func kmpEq1[K ~int64, V comparable](aV V, bS Sequence[K, V], bI K) bool {
	bV, bErr := bS.Get(bI)
	if bErr != nil {
		//nolint:errorlint // The == is just a fast-path; we still fall back to errors.Is.
		if bErr == ErrWildcard || errors.Is(bErr, ErrWildcard) {
			return true
		}
		panic(bErr)
	}
	return aV == bV
}

// buildKMPTable takes the string 'substr', and returns a table such
// that 'table[matchLen-1]' is the largest value 'val' for which 'val < matchLen' and
// 'substr[:val] == substr[matchLen-val:matchLen]'.
func buildKMPTable[K ~int64, V comparable](substr Sequence[K, V]) ([]K, error) {
	var substrLen K
	for {
		//nolint:errorlint // The == is just a fast-path; we still fall back to errors.Is.
		if _, err := substr.Get(substrLen); err != nil && !(err == ErrWildcard || errors.Is(err, ErrWildcard)) {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		substrLen++
	}

	table := make([]K, substrLen)
	for j := K(0); j < substrLen; j++ {
		if j == 0 {
			// First entry must always be 0 (in order to
			// satisfy 'val < matchLen').
			continue
		}
		val := table[j-1]
		// not a match; go back
		for val > 0 && !kmpEq2(substr, j, substr, val) {
			val = table[val-1]
		}
		// is a match; go forward
		if kmpEq2(substr, val, substr, j) {
			val++
		}
		table[j] = val
	}
	return table, nil
}

// IndexAll returns the starting-position of all possibly-overlapping
// occurances of 'substr' in the 'str' sequence.
//
// Will hop around in 'substr', but will only get the natural sequence
// [0...) in order from 'str'.  When hopping around in 'substr' it
// assumes that once it has gotten a given index without error, it can
// continue to do so without error; errors appearing later will cause
// panics.
//
// Will panic if the length of 'substr' is 0.
//
// The 'substr' may include wildcard characters by returning
// ErrWildcard for a position.
//
// Uses the Knuth-Morris-Pratt algorithm.
func IndexAll[K ~int64, V comparable](str, substr Sequence[K, V]) ([]K, error) {
	table, err := buildKMPTable(substr)
	if err != nil {
		return nil, err
	}
	substrLen := K(len(table))
	if substrLen == 0 {
		panic(errors.New("diskio.IndexAll: empty substring"))
	}

	var matches []K
	var curMatchBeg K
	var curMatchLen K

	for pos := K(0); ; pos++ {
		chr, err := str.Get(pos)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return matches, err
		}

		// Consider 'chr'
		for curMatchLen > 0 && !kmpEq1(chr, substr, curMatchLen) { // shorten the match
			overlap := table[curMatchLen-1]
			curMatchBeg += curMatchLen - overlap
			curMatchLen = overlap
		}
		if kmpEq1(chr, substr, curMatchLen) { // lengthen the match
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
}
