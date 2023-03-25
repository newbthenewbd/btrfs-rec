// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package caching

import (
	"fmt"
)

// LinkedListEntry [T] is an entry in a LinkedList [T].
type LinkedListEntry[T any] struct {
	list         *LinkedList[T]
	older, newer *LinkedListEntry[T]
	Value        T
}

func (entry *LinkedListEntry[T]) Older() *LinkedListEntry[T] { return entry.older }
func (entry *LinkedListEntry[T]) Newer() *LinkedListEntry[T] { return entry.newer }

// LinkedList is a doubly-linked list.
//
// Rather than "head/tail", "front/back", or "next/prev", it has
// "oldest" and "newest".  This is for to make code using it clearer;
// as the motivation for the LinkedList is as an implementation detail
// in LRU caches and FIFO queues, where this temporal naming is
// meaningful.  Similarly, it does not implement many common features
// of a linked-list, because these applications do not need such
// features.
//
// Compared to `containers/list.List`, LinkedList has the
// disadvantages that it has fewer safety checks and fewer features in
// general.
type LinkedList[T any] struct {
	oldest, newest *LinkedListEntry[T]
}

// IsEmpty returns whether the list empty or not.
func (l *LinkedList[T]) IsEmpty() bool {
	return l.oldest == nil
}

// Delete removes an entry from the list.  The entry is invalid once
// Delete returns, and should not be reused or have its .Value
// accessed.
//
// It is invalid (runtime-panic) to call Delete on a nil entry.
//
// It is invalid (runtime-panic) to call Delete on an entry that
// isn't in the list.
func (l *LinkedList[T]) Delete(entry *LinkedListEntry[T]) {
	if entry.list != l {
		panic(fmt.Errorf("LinkedList.Delete: entry %p not in list", entry))
	}
	if entry.newer == nil {
		l.newest = entry.older
	} else {
		entry.newer.older = entry.older
	}
	if entry.older == nil {
		l.oldest = entry.newer
	} else {
		entry.older.newer = entry.newer
	}

	// no memory leaks
	entry.list = nil
	entry.older = nil
	entry.newer = nil
}

// Store appends a value to the "newest" end of the list, returning
// the created entry.
//
// It is invalid (runtime-panic) to call Store on a nil entry.
//
// It is invalid (runtime-panic) to call Store on an entry that is
// already in a list.
func (l *LinkedList[T]) Store(entry *LinkedListEntry[T]) {
	if entry.list != nil {
		panic(fmt.Errorf("LinkedList.Store: entry %p is already in a list", entry))
	}
	entry.list = l
	entry.older = l.newest
	l.newest = entry
	if entry.older == nil {
		l.oldest = entry
	} else {
		entry.older.newer = entry
	}
}

// MoveToNewest moves an entry fron any position in the list to the
// "newest" end of the list.  If the entry is already in the "newest"
// position, then MoveToNewest is a no-op.
//
// It is invalid (runtime-panic) to call MoveToNewest on a nil entry.
//
// It is invalid (runtime-panic) to call MoveToNewest on an entry that
// isn't in the list.
func (l *LinkedList[T]) MoveToNewest(entry *LinkedListEntry[T]) {
	if entry.list != l {
		panic(fmt.Errorf("LinkedList.MoveToNewest: entry %p not in list", entry))
	}
	if entry.newer == nil {
		// Already newest.
		return
	}
	entry.newer.older = entry.older
	if entry.older == nil {
		l.oldest = entry.newer
	} else {
		entry.older.newer = entry.newer
	}

	entry.older = l.newest
	l.newest.newer = entry

	entry.newer = nil
	l.newest = entry
}

// Oldest returns the entry at the "oldest" end of the list, or nil if
// the list is empty.
func (l *LinkedList[T]) Oldest() *LinkedListEntry[T] {
	return l.oldest
}
