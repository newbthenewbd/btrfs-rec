// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes_test

/*
import (
	"strings"
	"testing"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/stretchr/testify/assert"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfstree"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect/rebuildnodes"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
)

func TestEncodeRebuiltNodes(t *testing.T) {
	dat := map[btrfsvol.LogicalAddr]*rebuildnodes.RebuiltNode{
		100007133184: {
			Errs: containers.NewSet[string](
				"btrfs.ReadNode: node@0x0000001748e3c000: expected generation\u003c=6596014 but claims to be generation=6596025",
			),
			MinKey: btrfsprim.Key{},
			MaxKey: btrfsprim.Key{},
			InTrees: containers.NewSet[btrfsprim.ObjID](
				257,
			),
			Node: btrfstree.Node{},
		},
	}
	var buf strings.Builder
	assert.NoError(t, lowmemjson.Encode(&lowmemjson.ReEncoder{
		Out: &buf,

		Indent:                "\t",
		ForceTrailingNewlines: true,
	}, dat))
	assert.Equal(t, `{
	"100007133184": {
		"Errs": [
			"btrfs.ReadNode: node@0x0000001748e3c000: expected generation\u003c=6596014 but claims to be generation=6596025"
		],
		"MinKey": {
			"ObjectID": 0,
			"ItemType": 0,
			"Offset": 0
		},
		"MaxKey": {
			"ObjectID": 0,
			"ItemType": 0,
			"Offset": 0
		},
		"InTrees": [
			257
		],
		"Size": 0,
		"ChecksumType": 0,
		"Head": {
			"Checksum": "0000000000000000000000000000000000000000000000000000000000000000",
			"MetadataUUID": "00000000-0000-0000-0000-000000000000",
			"Addr": 0,
			"Flags": 0,
			"BackrefRev": 0,
			"ChunkTreeUUID": "00000000-0000-0000-0000-000000000000",
			"Generation": 0,
			"Owner": 0,
			"NumItems": 0,
			"Level": 0
		},
		"BodyInternal": null,
		"BodyLeaf": null,
		"Padding": null
	}
}
`, buf.String())
}
*/
