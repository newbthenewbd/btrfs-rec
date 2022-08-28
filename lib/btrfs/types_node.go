// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfs

import (
	"encoding/binary"
	"errors"
	"fmt"

	"git.lukeshu.com/btrfs-progs-ng/lib/binstruct"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsitem"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfssum"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/diskio"
	"git.lukeshu.com/btrfs-progs-ng/lib/fmtutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

type NodeFlags uint64

func (NodeFlags) BinaryStaticSize() int {
	return 7
}
func (f NodeFlags) MarshalBinary() ([]byte, error) {
	var bs [8]byte
	binary.LittleEndian.PutUint64(bs[:], uint64(f))
	return bs[:7], nil
}
func (f *NodeFlags) UnmarshalBinary(dat []byte) (int, error) {
	var bs [8]byte
	copy(bs[:7], dat[:7])
	*f = NodeFlags(binary.LittleEndian.Uint64(bs[:]))
	return 7, nil
}

var (
	_ binstruct.StaticSizer = NodeFlags(0)
	_ binstruct.Marshaler   = NodeFlags(0)
	_ binstruct.Unmarshaler = (*NodeFlags)(nil)
)

const (
	NodeWritten = NodeFlags(1 << iota)
	NodeReloc
)

var nodeFlagNames = []string{
	"WRITTEN",
	"RELOC",
}

func (f NodeFlags) Has(req NodeFlags) bool { return f&req == req }
func (f NodeFlags) String() string         { return fmtutil.BitfieldString(f, nodeFlagNames, fmtutil.HexLower) }

type BackrefRev uint8

const (
	OldBackrefRev   = BackrefRev(iota)
	MixedBackrefRev = BackrefRev(iota)
)

// Node: main //////////////////////////////////////////////////////////////////////////////////////

type Node struct {
	// Some context from the parent filesystem
	Size         uint32            // superblock.NodeSize
	ChecksumType btrfssum.CSumType // superblock.ChecksumType

	// The node's header (always present)
	Head NodeHeader

	// The node's body (which one of these is present depends on
	// the node's type, as specified in the header)
	BodyInternal []KeyPointer // for internal nodes
	BodyLeaf     []Item       // for leave nodes

	Padding []byte
}

type NodeHeader struct {
	Checksum      btrfssum.CSum        `bin:"off=0x0,  siz=0x20"`
	MetadataUUID  UUID                 `bin:"off=0x20, siz=0x10"`
	Addr          btrfsvol.LogicalAddr `bin:"off=0x30, siz=0x8"` // Logical address of this node
	Flags         NodeFlags            `bin:"off=0x38, siz=0x7"`
	BackrefRev    BackrefRev           `bin:"off=0x3f, siz=0x1"`
	ChunkTreeUUID UUID                 `bin:"off=0x40, siz=0x10"`
	Generation    Generation           `bin:"off=0x50, siz=0x8"`
	Owner         ObjID                `bin:"off=0x58, siz=0x8"` // The ID of the tree that contains this node
	NumItems      uint32               `bin:"off=0x60, siz=0x4"` // [ignored-when-writing]
	Level         uint8                `bin:"off=0x64, siz=0x1"` // 0 for leaf nodes, >=1 for internal nodes
	binstruct.End `bin:"off=0x65"`
}

// MaxItems returns the maximum possible valid value of
// .Head.NumItems.
func (node Node) MaxItems() uint32 {
	bodyBytes := node.Size - uint32(binstruct.StaticSize(NodeHeader{}))
	if node.Head.Level > 0 {
		return bodyBytes / uint32(binstruct.StaticSize(KeyPointer{}))
	} else {
		return bodyBytes / uint32(binstruct.StaticSize(ItemHeader{}))
	}
}

func (node Node) MinItem() (Key, bool) {
	if node.Head.Level > 0 {
		if len(node.BodyInternal) == 0 {
			return Key{}, false
		}
		return node.BodyInternal[0].Key, true
	} else {
		if len(node.BodyLeaf) == 0 {
			return Key{}, false
		}
		return node.BodyLeaf[0].Key, true
	}
}

func (node Node) MaxItem() (Key, bool) {
	if node.Head.Level > 0 {
		if len(node.BodyInternal) == 0 {
			return Key{}, false
		}
		return node.BodyInternal[len(node.BodyInternal)-1].Key, true
	} else {
		if len(node.BodyLeaf) == 0 {
			return Key{}, false
		}
		return node.BodyLeaf[len(node.BodyLeaf)-1].Key, true
	}
}

func (node Node) CalculateChecksum() (btrfssum.CSum, error) {
	data, err := binstruct.Marshal(node)
	if err != nil {
		return btrfssum.CSum{}, err
	}
	return node.ChecksumType.Sum(data[binstruct.StaticSize(btrfssum.CSum{}):])
}

func (node Node) ValidateChecksum() error {
	stored := node.Head.Checksum
	calced, err := node.CalculateChecksum()
	if err != nil {
		return err
	}
	if calced != stored {
		return fmt.Errorf("node checksum mismatch: stored=%v calculated=%v",
			stored, calced)
	}
	return nil
}

func (node *Node) UnmarshalBinary(nodeBuf []byte) (int, error) {
	*node = Node{
		Size:         uint32(len(nodeBuf)),
		ChecksumType: node.ChecksumType,
	}
	if len(nodeBuf) <= binstruct.StaticSize(NodeHeader{}) {
		return 0, fmt.Errorf("size must be greater than %v, but is %v",
			binstruct.StaticSize(NodeHeader{}),
			len(nodeBuf))
	}
	n, err := binstruct.Unmarshal(nodeBuf, &node.Head)
	if err != nil {
		return n, err
	} else if n != binstruct.StaticSize(NodeHeader{}) {
		return n, fmt.Errorf("header consumed %v bytes but expected %v",
			n, binstruct.StaticSize(NodeHeader{}))
	}
	if node.Head.Level > 0 {
		_n, err := node.unmarshalInternal(nodeBuf[n:])
		n += _n
		if err != nil {
			return n, fmt.Errorf("internal: %w", err)
		}
	} else {
		_n, err := node.unmarshalLeaf(nodeBuf[n:])
		n += _n
		if err != nil {
			return n, fmt.Errorf("leaf: %w", err)
		}
	}
	if n != len(nodeBuf) {
		return n, fmt.Errorf("left over data: got %v bytes but only consumed %v",
			len(nodeBuf), n)
	}
	return n, nil
}

func (node Node) MarshalBinary() ([]byte, error) {
	if node.Size == 0 {
		return nil, fmt.Errorf(".Size must be set")
	}
	if node.Size <= uint32(binstruct.StaticSize(NodeHeader{})) {
		return nil, fmt.Errorf(".Size must be greater than %v, but is %v",
			binstruct.StaticSize(NodeHeader{}),
			node.Size)
	}
	if node.Head.Level > 0 {
		node.Head.NumItems = uint32(len(node.BodyInternal))
	} else {
		node.Head.NumItems = uint32(len(node.BodyLeaf))
	}

	buf := make([]byte, node.Size)

	if bs, err := binstruct.Marshal(node.Head); err != nil {
		return buf, err
	} else if len(bs) != binstruct.StaticSize(NodeHeader{}) {
		return nil, fmt.Errorf("header is %v bytes but expected %v",
			len(bs), binstruct.StaticSize(NodeHeader{}))
	} else {
		copy(buf, bs)
	}

	if node.Head.Level > 0 {
		if err := node.marshalInternalTo(buf[binstruct.StaticSize(NodeHeader{}):]); err != nil {
			return buf, err
		}
	} else {
		if err := node.marshalLeafTo(buf[binstruct.StaticSize(NodeHeader{}):]); err != nil {
			return buf, err
		}
	}

	return buf, nil
}

// Node: "internal" ////////////////////////////////////////////////////////////////////////////////

type KeyPointer struct {
	Key           Key                  `bin:"off=0x0, siz=0x11"`
	BlockPtr      btrfsvol.LogicalAddr `bin:"off=0x11, siz=0x8"`
	Generation    Generation           `bin:"off=0x19, siz=0x8"`
	binstruct.End `bin:"off=0x21"`
}

func (node *Node) unmarshalInternal(bodyBuf []byte) (int, error) {
	n := 0
	for i := uint32(0); i < node.Head.NumItems; i++ {
		var item KeyPointer
		_n, err := binstruct.Unmarshal(bodyBuf[n:], &item)
		n += _n
		if err != nil {
			return n, fmt.Errorf("item %v: %w", i, err)
		}
		node.BodyInternal = append(node.BodyInternal, item)
	}
	node.Padding = bodyBuf[n:]
	return len(bodyBuf), nil
}

func (node *Node) marshalInternalTo(bodyBuf []byte) error {
	n := 0
	for i, item := range node.BodyInternal {
		bs, err := binstruct.Marshal(item)
		if err != nil {
			return fmt.Errorf("item %v: %w", i, err)
		}
		if copy(bodyBuf[n:], bs) < len(bs) {
			return fmt.Errorf("item %v: not enough space: need at least %v+%v=%v bytes, but only have %v",
				i, n, len(bs), n+len(bs), len(bodyBuf))
		}
		n += len(bs)
	}
	if copy(bodyBuf[n:], node.Padding) < len(node.Padding) {
		return fmt.Errorf("padding: not enough space: need at least %v+%v=%v bytes, but only have %v",
			n, len(node.Padding), n+len(node.Padding), len(bodyBuf))
	}
	return nil
}

// Node: "leaf" ////////////////////////////////////////////////////////////////////////////////////

type Item struct {
	Key      Key
	BodySize uint32 // [ignored-when-writing]
	Body     btrfsitem.Item
}

type ItemHeader struct {
	Key           Key    `bin:"off=0x0, siz=0x11"`
	DataOffset    uint32 `bin:"off=0x11, siz=0x4"` // [ignored-when-writing] relative to the end of the header (0x65)
	DataSize      uint32 `bin:"off=0x15, siz=0x4"` // [ignored-when-writing]
	binstruct.End `bin:"off=0x19"`
}

func (node *Node) unmarshalLeaf(bodyBuf []byte) (int, error) {
	head := 0
	tail := len(bodyBuf)
	for i := uint32(0); i < node.Head.NumItems; i++ {
		var itemHead ItemHeader
		n, err := binstruct.Unmarshal(bodyBuf[head:], &itemHead)
		head += n
		if err != nil {
			return 0, fmt.Errorf("item %v: head: %w", i, err)
		}
		if head > tail {
			return 0, fmt.Errorf("item %v: head: end_offset=%#x is in the body section (offset>%#x)",
				i, head, tail)
		}

		dataOff := int(itemHead.DataOffset)
		if dataOff < head {
			return 0, fmt.Errorf("item %v: body: beg_offset=%#x is in the head section (offset<%#x)",
				i, dataOff, head)
		}
		dataSize := int(itemHead.DataSize)
		if dataOff+dataSize != tail {
			return 0, fmt.Errorf("item %v: body: end_offset=%#x is not cur_tail=%#x)",
				i, dataOff+dataSize, tail)
		}
		tail = dataOff
		dataBuf := bodyBuf[dataOff : dataOff+dataSize]

		node.BodyLeaf = append(node.BodyLeaf, Item{
			Key:      itemHead.Key,
			BodySize: itemHead.DataSize,
			Body:     btrfsitem.UnmarshalItem(itemHead.Key, node.ChecksumType, dataBuf),
		})
	}

	node.Padding = bodyBuf[head:tail]
	return len(bodyBuf), nil
}

func (node *Node) marshalLeafTo(bodyBuf []byte) error {
	head := 0
	tail := len(bodyBuf)
	for i, item := range node.BodyLeaf {
		itemBodyBuf, err := binstruct.Marshal(item.Body)
		if err != nil {
			return fmt.Errorf("item %v: body: %w", i, err)
		}
		itemHeadBuf, err := binstruct.Marshal(ItemHeader{
			Key:        item.Key,
			DataSize:   uint32(len(itemBodyBuf)),
			DataOffset: uint32(tail - len(itemBodyBuf)),
		})
		if err != nil {
			return fmt.Errorf("item %v: head: %w", i, err)
		}

		if tail-head < len(itemHeadBuf)+len(itemBodyBuf) {
			return fmt.Errorf("item %v: not enough space: need at least (head_len:%v)+(body_len:%v)=%v free bytes, but only have %v",
				i, len(itemHeadBuf), len(itemBodyBuf), len(itemHeadBuf)+len(itemBodyBuf), tail-head)
		}

		copy(bodyBuf[head:], itemHeadBuf)
		head += len(itemHeadBuf)
		tail -= len(itemBodyBuf)
		copy(bodyBuf[tail:], itemBodyBuf)
	}
	if copy(bodyBuf[head:tail], node.Padding) < len(node.Padding) {
		return fmt.Errorf("padding: not enough space: need at least %v free bytes, but only have %v",
			len(node.Padding), tail-head)
	}
	return nil
}

func (node *Node) LeafFreeSpace() uint32 {
	if node.Head.Level > 0 {
		panic(fmt.Errorf("Node.LeafFreeSpace: not a leaf node"))
	}
	freeSpace := node.Size
	freeSpace -= uint32(binstruct.StaticSize(NodeHeader{}))
	for _, item := range node.BodyLeaf {
		freeSpace -= uint32(binstruct.StaticSize(ItemHeader{}))
		bs, _ := binstruct.Marshal(item.Body)
		freeSpace -= uint32(len(bs))
	}
	return freeSpace
}

// Tie Nodes in to the FS //////////////////////////////////////////////////////////////////////////

var ErrNotANode = errors.New("does not look like a node")

type NodeExpectations struct {
	LAddr containers.Optional[btrfsvol.LogicalAddr]
	// Things knowable from the parent.
	Level         containers.Optional[uint8]
	MaxGeneration containers.Optional[Generation]
	Owner         []ObjID
}

func ReadNode[Addr ~int64](fs diskio.File[Addr], sb Superblock, addr Addr, exp NodeExpectations) (*diskio.Ref[Addr, Node], error) {
	nodeBuf := make([]byte, sb.NodeSize)
	if _, err := fs.ReadAt(nodeBuf, addr); err != nil {
		return nil, err
	}

	// parse (early)

	nodeRef := &diskio.Ref[Addr, Node]{
		File: fs,
		Addr: addr,
		Data: Node{
			Size:         sb.NodeSize,
			ChecksumType: sb.ChecksumType,
		},
	}
	if _, err := binstruct.Unmarshal(nodeBuf, &nodeRef.Data.Head); err != nil {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: %w", addr, err)
	}

	// sanity checking

	if nodeRef.Data.Head.MetadataUUID != sb.EffectiveMetadataUUID() {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: %w", addr, ErrNotANode)
	}

	stored := nodeRef.Data.Head.Checksum
	calced, err := nodeRef.Data.ChecksumType.Sum(nodeBuf[binstruct.StaticSize(btrfssum.CSum{}):])
	if err != nil {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: %w", addr, err)
	}
	if stored != calced {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: looks like a node but is corrupt: checksum mismatch: stored=%v calculated=%v",
			addr, stored, calced)
	}

	if exp.LAddr.OK && nodeRef.Data.Head.Addr != exp.LAddr.Val {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: read from laddr=%v but claims to be at laddr=%v",
			addr, exp.LAddr.Val, nodeRef.Data.Head.Addr)
	}
	if exp.Level.OK && nodeRef.Data.Head.Level != exp.Level.Val {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: expected level=%v but claims to be level=%v",
			addr, exp.Level.Val, nodeRef.Data.Head.Level)
	}
	if exp.MaxGeneration.OK && nodeRef.Data.Head.Generation > exp.MaxGeneration.Val {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: expected generation<=%v but claims to be generation=%v",
			addr, exp.MaxGeneration.Val, nodeRef.Data.Head.Generation)
	}
	if len(exp.Owner) > 0 && !slices.Contains(nodeRef.Data.Head.Owner, exp.Owner) {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: expected owner in %v but claims to have owner=%v",
			addr, exp.Owner, nodeRef.Data.Head.Owner)
	}

	// parse (main)

	if _, err := binstruct.Unmarshal(nodeBuf, &nodeRef.Data); err != nil {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%v: %w", addr, err)
	}

	// return

	return nodeRef, nil
}
