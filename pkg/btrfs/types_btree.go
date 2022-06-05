package btrfs

import (
	"encoding/binary"
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
)

type Node struct {
	// Some context from the parent filesystem
	Size uint32 // superblock.NodeSize

	// The node's header (always present)
	Head NodeHeader

	// The node's body (which one of these is present depends on
	// the node's type, as specified in the header)
	BodyInternal []KeyPointer // for internal nodes
	BodyLeaf     []Item       // for leave nodes

	Padding []byte
}

type NodeHeader struct {
	Checksum      CSum        `bin:"off=0x0,  siz=0x20"` // Checksum of everything after this field (from 20 to the end of the node)
	MetadataUUID  UUID        `bin:"off=0x20, siz=0x10"` // FS UUID
	Addr          LogicalAddr `bin:"off=0x30, siz=0x8"`  // Logical address of this node
	Flags         NodeFlags   `bin:"off=0x38, siz=0x7"`
	BackrefRev    uint8       `bin:"off=0x3f, siz=0x1"`
	ChunkTreeUUID UUID        `bin:"off=0x40, siz=0x10"` // Chunk tree UUID
	Generation    Generation  `bin:"off=0x50, siz=0x8"`  // Generation
	Owner         ObjID       `bin:"off=0x58, siz=0x8"`  // The ID of the tree that contains this node
	NumItems      uint32      `bin:"off=0x60, siz=0x4"`  // Number of items
	Level         uint8       `bin:"off=0x64, siz=0x1"`  // Level (0 for leaf nodes)
	binstruct.End `bin:"off=0x65"`
}

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
func (f NodeFlags) String() string         { return util.BitfieldString(f, nodeFlagNames) }

type KeyPointer struct {
	Key           Key         `bin:"off=0x0, siz=0x11"`
	BlockPtr      LogicalAddr `bin:"off=0x11, siz=0x8"`
	Generation    Generation  `bin:"off=0x19, siz=0x8"`
	binstruct.End `bin:"off=0x21"`
}

type ItemHeader struct {
	Key           Key    `bin:"off=0x0, siz=0x11"`
	DataOffset    uint32 `bin:"off=0x11, siz=0x4"` // relative to the end of the header (0x65)
	DataSize      uint32 `bin:"off=0x15, siz=0x4"`
	binstruct.End `bin:"off=0x19"`
}

type Item struct {
	Head ItemHeader
	Body btrfsitem.Item
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

func (node *Node) UnmarshalBinary(nodeBuf []byte) (int, error) {
	n, err := binstruct.Unmarshal(nodeBuf, &node.Head)
	if err != nil {
		return n, err
	}
	if node.Head.Level > 0 {
		// internal node
		for i := uint32(0); i < node.Head.NumItems; i++ {
			var item KeyPointer
			_n, err := binstruct.Unmarshal(nodeBuf[n:], &item)
			n += _n
			if err != nil {
				return n, fmt.Errorf("(internal): item %d: %w", i, err)
			}
			node.BodyInternal = append(node.BodyInternal, item)
		}
		node.Padding = nodeBuf[n:]
		return len(nodeBuf), nil
	} else {
		// leaf node
		firstRead := len(nodeBuf)
		lastRead := 0
		for i := uint32(0); i < node.Head.NumItems; i++ {
			var item Item
			_n, err := binstruct.Unmarshal(nodeBuf[n:], &item.Head)
			n += _n
			if err != nil {
				return n, fmt.Errorf("(leaf): item %d: %w", i, err)
			}

			dataOff := binstruct.StaticSize(NodeHeader{}) + int(item.Head.DataOffset)
			dataSize := int(item.Head.DataSize)
			if dataOff+dataSize > len(nodeBuf) {
				return max(n, lastRead), fmt.Errorf("(leaf): item references byte %d, but node only has %d bytes",
					dataOff+dataSize, len(nodeBuf))
			}
			dataBuf := nodeBuf[dataOff : dataOff+dataSize]
			firstRead = min(firstRead, dataOff)
			lastRead = max(lastRead, dataOff+dataSize)
			item.Body = btrfsitem.UnmarshalItem(item.Head.Key.ItemType, dataBuf)

			node.BodyLeaf = append(node.BodyLeaf, item)
		}
		node.Padding = nodeBuf[n:firstRead]
		return max(n, lastRead), nil
	}
}

func (node Node) MarshalBinary() ([]byte, error) {
	if node.Size == 0 {
		return nil, fmt.Errorf("Node.MarshalBinary: .Size must be set")
	}

	ret := make([]byte, node.Size)

	dat, err := binstruct.Marshal(node.Head)
	if err != nil {
		return dat, err
	}
	if node.Head.Level > 0 {
		// internal node
		for _, item := range node.BodyInternal {
			bs, err := binstruct.Marshal(item)
			dat = append(dat, bs...)
			if err != nil {
				return dat, err
			}
		}
		dat = append(dat, node.Padding...)
		if copy(ret, dat) < len(dat) {
			return ret, fmt.Errorf("btrfs.Node.MarshalBinary: need at least %d bytes, but .Size is only %d",
				len(dat), node.Size)
		}
	} else {
		// leaf node
		if copy(ret, dat) < len(dat) {
			return ret, fmt.Errorf("btrfs.Node.MarshalBinary: need at least %d bytes, but .Size is only %d",
				len(dat), node.Size)
		}
		n := len(dat)
		minData := len(ret)
		for _, item := range node.BodyLeaf {
			dat, err = binstruct.Marshal(item.Head)
			if err != nil {
				return ret, err
			}
			if copy(ret[n:], dat) < len(dat) {
				return ret, fmt.Errorf("btrfs.Node.MarshalBinary: need at least %d bytes, but .Size is only %d",
					n+len(dat), node.Size)
			}
			n += len(dat)

			dat, err := binstruct.Marshal(item.Body)
			if err != nil {
				return ret, err
			}
			dataOff := binstruct.StaticSize(NodeHeader{}) + int(item.Head.DataOffset)
			minData = min(minData, dataOff)
			if copy(ret[dataOff:], dat) < len(dat) {
				return ret, fmt.Errorf("btrfs.Node.MarshalBinary: need at least %d bytes, but .Size is only %d",
					dataOff+len(dat), node.Size)
			}
		}
		if copy(ret[n:minData], node.Padding) < len(node.Padding) {
			return ret, fmt.Errorf("btrfs.Node.MarshalBinary: not enough room left for padding")
		}

	}
	return ret, nil
}

func (node Node) CalculateChecksum() (CSum, error) {
	data, err := binstruct.Marshal(node)
	if err != nil {
		return CSum{}, err
	}
	return CRC32c(data[binstruct.StaticSize(CSum{}):]), nil
}

func (node Node) ValidateChecksum() error {
	stored := node.Head.Checksum
	calced, err := node.CalculateChecksum()
	if err != nil {
		return err
	}
	if !calced.Equal(stored) {
		return fmt.Errorf("node checksum mismatch: stored=%s calculated=%s",
			stored, calced)
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
		freeSpace -= item.Head.DataSize
	}
	return freeSpace
}

func (fs *FS) ReadNode(addr LogicalAddr) (util.Ref[LogicalAddr, Node], error) {
	var ret util.Ref[LogicalAddr, Node]

	sb, err := fs.Superblock()
	if err != nil {
		return ret, fmt.Errorf("btrfs.FS.ReadNode: %w", err)
	}

	// read

	nodeBuf := make([]byte, sb.Data.NodeSize)
	if _, err := fs.ReadAt(nodeBuf, addr); err != nil {
		return ret, err
	}

	var node Node
	node.Size = sb.Data.NodeSize

	if _, err := node.UnmarshalBinary(nodeBuf); err != nil {
		return ret, fmt.Errorf("btrfs.FS.ReadNode: node@%d: %w", addr, err)
	}

	// sanity checking

	if !node.Head.MetadataUUID.Equal(sb.Data.EffectiveMetadataUUID()) {
		return ret, fmt.Errorf("btrfs.FS.ReadNode: node@%d: does not look like a node", addr)
	}

	if node.Head.Addr != addr {
		return ret, fmt.Errorf("btrfs.FS.ReadNode: node@%d: read from laddr=%d but claims to be at laddr=%d",
			addr, addr, node.Head.Addr)
	}

	stored := node.Head.Checksum
	calced := CRC32c(nodeBuf[binstruct.StaticSize(CSum{}):])
	if !calced.Equal(stored) {
		return ret, fmt.Errorf("btrfs.FS.ReadNode: node@%d: checksum mismatch: stored=%s calculated=%s",
			addr, stored, calced)
	}

	// return

	return util.Ref[LogicalAddr, Node]{
		File: fs,
		Addr: addr,
		Data: node,
	}, nil
}

type WalkTreeHandler struct {
	// Callbacks for items on internal nodes
	PreKeyPointer  func(KeyPointer) error
	PostKeyPointer func(KeyPointer) error
	// Callbacks for items on leaf nodes
	Item func(Key, btrfsitem.Item) error
	// Error handler
	NodeError func(error) error
}

func (fs *FS) WalkTree(nodeAddr LogicalAddr, cbs WalkTreeHandler) error {
	if nodeAddr == 0 {
		return nil
	}
	node, err := fs.ReadNode(nodeAddr)
	if err != nil {
		if cbs.NodeError != nil {
			err = cbs.NodeError(err)
		}
		if err != nil {
			return fmt.Errorf("btrfs.FS.WalkTree: %w", err)
		}
	}
	for _, item := range node.Data.BodyInternal {
		if cbs.PreKeyPointer != nil {
			if err := cbs.PreKeyPointer(item); err != nil {
				return err
			}
		}
		if err := fs.WalkTree(item.BlockPtr, cbs); err != nil {
			return err
		}
		if cbs.PostKeyPointer != nil {
			if err := cbs.PostKeyPointer(item); err != nil {
				return err
			}
		}
	}
	for _, item := range node.Data.BodyLeaf {
		if cbs.Item != nil {
			if err := cbs.Item(item.Head.Key, item.Body); err != nil {
				return fmt.Errorf("btrfs.FS.WalkTree: callback: %w", err)
			}
		}
	}
	return nil
}
