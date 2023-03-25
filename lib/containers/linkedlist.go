// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package containers

import (
	"fmt"
)

// LinkedListEntry [T] is an entry in a LinkedList [T].
type LinkedListEntry[T any] struct {
	List         *LinkedList[T]
	Older, Newer *LinkedListEntry[T]
	Value        T
}

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
	Len            int
	Oldest, Newest *LinkedListEntry[T]
}

// IsEmpty returns whether the list empty or not.
func (l *LinkedList[T]) IsEmpty() bool {
	return l.Oldest == nil
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
	if entry.List != l {
		panic(fmt.Errorf("LinkedList.Delete: entry %p not in list", entry))
	}
	l.Len--
	if entry.Newer == nil {
		l.Newest = entry.Older
	} else {
		entry.Newer.Older = entry.Older
	}
	if entry.Older == nil {
		l.Oldest = entry.Newer
	} else {
		entry.Older.Newer = entry.Newer
	}

	// no memory leaks
	entry.List = nil
	entry.Older = nil
	entry.Newer = nil
}

// Store appends a value to the "newest" end of the list, returning
// the created entry.
//
// It is invalid (runtime-panic) to call Store on a nil entry.
//
// It is invalid (runtime-panic) to call Store on an entry that is
// already in a list.
func (l *LinkedList[T]) Store(entry *LinkedListEntry[T]) {
	if entry.List != nil {
		panic(fmt.Errorf("LinkedList.Store: entry %p is already in a list", entry))
	}
	l.Len++
	entry.List = l
	entry.Older = l.Newest
	l.Newest = entry
	if entry.Older == nil {
		l.Oldest = entry
	} else {
		entry.Older.Newer = entry
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
	if entry.List != l {
		panic(fmt.Errorf("LinkedList.MoveToNewest: entry %p not in list", entry))
	}
	if entry.Newer == nil {
		// Already newest.
		return
	}
	entry.Newer.Older = entry.Older
	if entry.Older == nil {
		l.Oldest = entry.Newer
	} else {
		entry.Older.Newer = entry.Newer
	}

	entry.Older = l.Newest
	l.Newest.Newer = entry

	entry.Newer = nil
	l.Newest = entry
}
