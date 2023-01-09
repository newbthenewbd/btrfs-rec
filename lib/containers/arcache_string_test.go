// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (c *ARCache[K, V]) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	keys := make([]string, 0, c.fullLen())
	for entry := c.recentGhost.byAge.oldest; entry != nil; entry = entry.newer {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}
	for entry := c.recentLive.byAge.oldest; entry != nil; entry = entry.newer {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}
	for entry := c.frequentLive.byAge.newest; entry != nil; entry = entry.older {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}
	for entry := c.frequentGhost.byAge.newest; entry != nil; entry = entry.older {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}

	keyLen := 3
	for _, key := range keys {
		keyLen = max(keyLen, len(key))
	}

	var out strings.Builder
	blankLeft := c.MaxLen - c.recentLen()
	for i := 0; i <= 2*c.MaxLen; i++ {
		sep := []byte("    ")
		if i == blankLeft+c.recentGhost.Len() {
			sep[0] = '['
		}
		if i == blankLeft+c.recentGhost.Len()+c.recentLive.Len() {
			sep[1] = '!'
		}
		if i == blankLeft+c.recentGhost.Len()+c.recentLive.Len()-c.recentLiveTarget {
			sep[2] = '^'
		}
		if i == blankLeft+c.recentGhost.Len()+c.recentLive.Len()+c.frequentLive.Len() {
			sep[3] = ']'
		}
		out.Write(sep)

		if i < 2*c.MaxLen {
			key := ""
			if i >= blankLeft && i < blankLeft+c.fullLen() {
				key = keys[i-blankLeft]
			}
			spaceLeft := (keyLen - len(key)) / 2
			out.WriteString(strings.Repeat("_", spaceLeft))
			out.WriteString(key)
			out.WriteString(strings.Repeat("_", keyLen-len(key)-spaceLeft))
		}
	}

	return out.String()
}

func TestARCacheString(t *testing.T) {
	t.Parallel()
	cache := &ARCache[int, int]{
		MaxLen: 4,
	}

	assert.Equal(t, `    ___    ___    ___    ___[!^]___    ___    ___    ___    `, cache.String())
}
