// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfstree_test

import (
	"errors"
	"fmt"
	iofs "io/fs"
	"testing"

	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
)

func TestErrs(t *testing.T) {
	t.Parallel()

	errItem := fmt.Errorf("my item: %w", btrfstree.ErrNoItem)
	errTree := fmt.Errorf("my tree: %w", btrfstree.ErrNoTree)

	// 1. errItem
	// 2. errTree
	// 3. btrfstree.ErrNoItem
	// 4. btrfstree.ErrNoTree
	// 5. iofs.ErrNotExist

	// 1
	assert.Equal(t, errors.Is(errItem, errItem), true)
	assert.Equal(t, errors.Is(errItem, errTree), false)
	assert.Equal(t, errors.Is(errItem, btrfstree.ErrNoItem), true)
	assert.Equal(t, errors.Is(errItem, btrfstree.ErrNoTree), false)
	assert.Equal(t, errors.Is(errItem, iofs.ErrNotExist), true)

	// 2
	assert.Equal(t, errors.Is(errTree, errItem), false)
	assert.Equal(t, errors.Is(errTree, errTree), true)
	assert.Equal(t, errors.Is(errTree, btrfstree.ErrNoItem), false)
	assert.Equal(t, errors.Is(errTree, btrfstree.ErrNoTree), true)
	assert.Equal(t, errors.Is(errTree, iofs.ErrNotExist), true)

	// 3
	assert.Equal(t, errors.Is(btrfstree.ErrNoItem, errItem), false)
	assert.Equal(t, errors.Is(btrfstree.ErrNoItem, errTree), false)
	assert.Equal(t, errors.Is(btrfstree.ErrNoItem, btrfstree.ErrNoItem), true)
	assert.Equal(t, errors.Is(btrfstree.ErrNoItem, btrfstree.ErrNoTree), false)
	assert.Equal(t, errors.Is(btrfstree.ErrNoItem, iofs.ErrNotExist), true)

	// 4
	assert.Equal(t, errors.Is(btrfstree.ErrNoTree, errItem), false)
	assert.Equal(t, errors.Is(btrfstree.ErrNoTree, errTree), false)
	assert.Equal(t, errors.Is(btrfstree.ErrNoTree, btrfstree.ErrNoItem), false)
	assert.Equal(t, errors.Is(btrfstree.ErrNoTree, btrfstree.ErrNoTree), true)
	assert.Equal(t, errors.Is(btrfstree.ErrNoTree, iofs.ErrNotExist), true)

	// 5
	assert.Equal(t, errors.Is(iofs.ErrNotExist, errItem), false)
	assert.Equal(t, errors.Is(iofs.ErrNotExist, errTree), false)
	assert.Equal(t, errors.Is(iofs.ErrNotExist, btrfstree.ErrNoItem), false)
	assert.Equal(t, errors.Is(iofs.ErrNotExist, btrfstree.ErrNoTree), false)
	assert.Equal(t, errors.Is(iofs.ErrNotExist, iofs.ErrNotExist), true)
}
