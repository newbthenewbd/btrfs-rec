// Copyright (C) 2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"context"
	"testing"

	"github.com/datawire/dlib/dlog"
	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

type rebuiltForrestCallbacks struct {
	addedItem  func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key)
	addedRoot  func(ctx context.Context, tree btrfsprim.ObjID, root btrfsvol.LogicalAddr)
	lookupRoot func(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool)
	lookupUUID func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool)
}

func (cbs rebuiltForrestCallbacks) AddedItem(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
	cbs.addedItem(ctx, tree, key)
}

func (cbs rebuiltForrestCallbacks) AddedRoot(ctx context.Context, tree btrfsprim.ObjID, root btrfsvol.LogicalAddr) {
	cbs.addedRoot(ctx, tree, root)
}

func (cbs rebuiltForrestCallbacks) LookupRoot(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
	return cbs.lookupRoot(ctx, tree)
}

func (cbs rebuiltForrestCallbacks) LookupUUID(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
	return cbs.lookupUUID(ctx, uuid)
}

func TestRebuiltTreeCycles(t *testing.T) {
	t.Parallel()

	ctx := dlog.NewTestContext(t, true)

	type mockRoot struct {
		ID         btrfsprim.ObjID
		UUID       btrfsprim.UUID
		ParentUUID btrfsprim.UUID
		ParentGen  btrfsprim.Generation
	}
	roots := []mockRoot{
		{
			ID:         306,
			UUID:       btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000006"),
			ParentUUID: btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000005"),
			ParentGen:  1005,
		},
		{
			ID:         305,
			UUID:       btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000005"),
			ParentUUID: btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000004"),
			ParentGen:  1004,
		},
		{
			ID:         304,
			UUID:       btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000004"),
			ParentUUID: btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000003"),
			ParentGen:  1003,
		},
		{
			ID:         303,
			UUID:       btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000003"),
			ParentUUID: btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000005"),
			ParentGen:  1002,
		},
	}

	cbs := rebuiltForrestCallbacks{
		addedItem: func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
			// do nothing
		},
		addedRoot: func(ctx context.Context, tree btrfsprim.ObjID, root btrfsvol.LogicalAddr) {
			// do nothing
		},
		lookupRoot: func(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
			for _, root := range roots {
				if root.ID == tree {
					return root.ParentGen, btrfsitem.Root{
						Generation: 2000,
						UUID:       root.UUID,
						ParentUUID: root.ParentUUID,
					}, true
				}
			}
			return 0, btrfsitem.Root{}, false
		},
		lookupUUID: func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
			for _, root := range roots {
				if root.UUID == uuid {
					return root.ID, true
				}
			}
			return 0, false
		},
	}

	rfs := NewRebuiltForrest(nil, Graph{}, cbs)

	tree, err := rfs.RebuiltTree(ctx, 306)
	assert.EqualError(t, err, `loop detected: [306 305 304 303 305]`)
	assert.Nil(t, tree)

	assert.NotNil(t, rfs.trees[305])
	tree, err = rfs.RebuiltTree(ctx, 305)
	assert.EqualError(t, err, `loop detected: [305 304 303 305]`)
	assert.Nil(t, tree)

	assert.NotNil(t, rfs.trees[304])
	tree, err = rfs.RebuiltTree(ctx, 304)
	assert.EqualError(t, err, `loop detected: [304 303 305 304]`)
	assert.Nil(t, tree)

	assert.NotNil(t, rfs.trees[303])
	tree, err = rfs.RebuiltTree(ctx, 303)
	assert.EqualError(t, err, `loop detected: [303 305 304 303]`)
	assert.Nil(t, tree)
}

func TestRebuiltTreeParentErr(t *testing.T) {
	t.Parallel()

	ctx := dlog.NewTestContext(t, true)

	type mockRoot struct {
		ID         btrfsprim.ObjID
		UUID       btrfsprim.UUID
		ParentUUID btrfsprim.UUID
		ParentGen  btrfsprim.Generation
	}
	roots := []mockRoot{
		{
			ID:         305,
			UUID:       btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000005"),
			ParentUUID: btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000004"),
			ParentGen:  1004,
		},
		{
			ID:   304,
			UUID: btrfsprim.MustParseUUID("00000000-0000-0000-0000-000000000004"),
		},
	}

	cbs := rebuiltForrestCallbacks{
		addedItem: func(ctx context.Context, tree btrfsprim.ObjID, key btrfsprim.Key) {
			// do nothing
		},
		addedRoot: func(ctx context.Context, tree btrfsprim.ObjID, root btrfsvol.LogicalAddr) {
			// do nothing
		},
		lookupRoot: func(ctx context.Context, tree btrfsprim.ObjID) (offset btrfsprim.Generation, item btrfsitem.Root, ok bool) {
			if tree == 304 {
				// Force a fault.
				return 0, btrfsitem.Root{}, false
			}
			for _, root := range roots {
				if root.ID == tree {
					return root.ParentGen, btrfsitem.Root{
						Generation: 2000,
						UUID:       root.UUID,
						ParentUUID: root.ParentUUID,
					}, true
				}
			}
			return 0, btrfsitem.Root{}, false
		},
		lookupUUID: func(ctx context.Context, uuid btrfsprim.UUID) (id btrfsprim.ObjID, ok bool) {
			for _, root := range roots {
				if root.UUID == uuid {
					return root.ID, true
				}
			}
			return 0, false
		},
	}

	rfs := NewRebuiltForrest(nil, Graph{}, cbs)

	tree, err := rfs.RebuiltTree(ctx, 305)
	assert.EqualError(t, err, `failed to rebuild parent tree: 304: tree does not exist`)
	assert.Nil(t, tree)
}
