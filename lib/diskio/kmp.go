// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package diskio

import (
	"errors"
	"io"
)

// buildKMPTable takes the string 'substr', and returns a table such
// that 'table[matchLen-1]' is the largest value 'val' for which 'val < matchLen' and
// 'substr[:val] == substr[matchLen-val:matchLen]'.
func buildKMPTable(substr []byte) []int {
	table := make([]int, len(substr))
	for j := range table {
		if j == 0 {
			// First entry must always be 0 (in order to
			// satisfy 'val < matchLen').
			continue
		}
		val := table[j-1]
		// not a match; go back
		for val > 0 && substr[j] != substr[val] {
			val = table[val-1]
		}
		// is a match; go forward
		if substr[val] == substr[j] {
			val++
		}
		table[j] = val
	}
	return table
}

// FindAll returns the starting-position of all possibly-overlapping
// occurances of 'substr' in the 'r' stream.
//
// Will panic if len(substr)==0.
//
// Uses the Knuth-Morris-Pratt algorithm.
func FindAll(r io.ByteReader, substr []byte) ([]int64, error) {
	if len(substr) == 0 {
		panic(errors.New("diskio.FindAll: empty substring"))
	}
	table := buildKMPTable(substr)

	var matches []int64
	var curMatchBeg int64
	var curMatchLen int

	pos := int64(-1) // if 'r' were a slice; define 'pos' such that 'chr=r[pos]'
	for {
		// I/O
		var chr byte
		chr, err := r.ReadByte()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = nil
			}
			return matches, err
		}
		pos++

		// Consider 'chr'
		for curMatchLen > 0 && chr != substr[curMatchLen] { // shorten the match
			overlap := table[curMatchLen-1]
			curMatchBeg += int64(curMatchLen - overlap)
			curMatchLen = overlap
		}
		if chr == substr[curMatchLen] { // lengthen the match
			if curMatchLen == 0 {
				curMatchBeg = pos
			}
			curMatchLen++
			if curMatchLen == len(substr) {
				matches = append(matches, curMatchBeg)
				overlap := table[curMatchLen-1]
				curMatchBeg += int64(curMatchLen - overlap)
				curMatchLen = overlap
			}
		}
	}
}
