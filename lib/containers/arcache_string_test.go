// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func (c *arCache[K, V]) String() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	fullLen := len(c.liveByName) + len(c.ghostByName)
	keys := make([]string, 0, fullLen)
	for entry := c.recentGhost.Oldest; entry != nil; entry = entry.Newer {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}
	for entry := c.recentLive.Oldest; entry != nil; entry = entry.Newer {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}
	for entry := c.frequentLive.Newest; entry != nil; entry = entry.Older {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}
	for entry := c.frequentGhost.Newest; entry != nil; entry = entry.Older {
		keys = append(keys, fmt.Sprint(entry.Value.key))
	}

	keyLen := 3
	for _, key := range keys {
		keyLen = max(keyLen, len(key))
	}

	var out strings.Builder
	blankLeft := c.cap - (c.recentLive.Len + c.recentGhost.Len)
	for i := 0; i <= 2*c.cap; i++ {
		sep := []byte("    ")
		if i == blankLeft+c.recentGhost.Len {
			sep[0] = '['
		}
		if i == blankLeft+c.recentGhost.Len+c.recentLive.Len {
			sep[1] = '!'
		}
		if i == blankLeft+c.recentGhost.Len+c.recentLive.Len-c.recentLiveTarget {
			sep[2] = '^'
		}
		if i == blankLeft+c.recentGhost.Len+c.recentLive.Len+c.frequentLive.Len {
			sep[3] = ']'
		}
		out.Write(sep)

		if i < 2*c.cap {
			key := ""
			if i >= blankLeft && i < blankLeft+fullLen {
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
	cache := NewARCache[int, int](4, SourceFunc[int, int](func(context.Context, int, *int) {})).(*arCache[int, int])

	assert.Equal(t, `    ___    ___    ___    ___[!^]___    ___    ___    ___    `, cache.String())
}
