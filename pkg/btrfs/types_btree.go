package btrfs

import (
	"encoding/binary"
	"errors"
	"fmt"
	iofs "io/fs"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/util"
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
func (f NodeFlags) String() string         { return util.BitfieldString(f, nodeFlagNames, util.HexLower) }

// Node: main //////////////////////////////////////////////////////////////////////////////////////

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
	Checksum      CSum        `bin:"off=0x0,  siz=0x20"`
	MetadataUUID  UUID        `bin:"off=0x20, siz=0x10"`
	Addr          LogicalAddr `bin:"off=0x30, siz=0x8"` // Logical address of this node
	Flags         NodeFlags   `bin:"off=0x38, siz=0x7"`
	BackrefRev    uint8       `bin:"off=0x3f, siz=0x1"`
	ChunkTreeUUID UUID        `bin:"off=0x40, siz=0x10"`
	Generation    Generation  `bin:"off=0x50, siz=0x8"`
	Owner         ObjID       `bin:"off=0x58, siz=0x8"` // The ID of the tree that contains this node
	NumItems      uint32      `bin:"off=0x60, siz=0x4"` // [ignored-when-writing]
	Level         uint8       `bin:"off=0x64, siz=0x1"` // 0 for leaf nodes, >=1 for internal nodes
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
	if calced != stored {
		return fmt.Errorf("node checksum mismatch: stored=%s calculated=%s",
			stored, calced)
	}
	return nil
}

func (node *Node) UnmarshalBinary(nodeBuf []byte) (int, error) {
	*node = Node{
		Size: uint32(len(nodeBuf)),
	}
	n, err := binstruct.Unmarshal(nodeBuf, &node.Head)
	if err != nil {
		return n, fmt.Errorf("btrfs.Node.UnmarshalBinary: %w", err)
	}
	if node.Head.Level > 0 {
		_n, err := node.unmarshalInternal(nodeBuf[n:])
		n += _n
		if err != nil {
			return n, fmt.Errorf("btrfs.Node.UnmarshalBinary: internal: %w", err)
		}
	} else {
		_n, err := node.unmarshalLeaf(nodeBuf[n:])
		n += _n
		if err != nil {
			return n, fmt.Errorf("btrfs.Node.UnmarshalBinary: leaf: %w", err)
		}
	}
	if n != len(nodeBuf) {
		return n, fmt.Errorf("btrfs.Node.UnmarshalBinary: left over data: got %d bytes but only consumed %d",
			len(nodeBuf), n)
	}
	return n, nil
}

func (node Node) MarshalBinary() ([]byte, error) {
	if node.Size == 0 {
		return nil, fmt.Errorf("btrfs.Node.MarshalBinary: .Size must be set")
	}
	if node.Size <= uint32(binstruct.StaticSize(NodeHeader{})) {
		return nil, fmt.Errorf("btrfs.Node.MarshalBinary: .Size must be greater than %d",
			binstruct.StaticSize(NodeHeader{}))
	}

	buf := make([]byte, node.Size)

	if bs, err := binstruct.Marshal(node.Head); err != nil {
		return buf, err
	} else if len(bs) != binstruct.StaticSize(NodeHeader{}) {
		return nil, fmt.Errorf("btrfs.Node.MarshalBinary: header is %d bytes but expected %d",
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
	Key           Key         `bin:"off=0x0, siz=0x11"`
	BlockPtr      LogicalAddr `bin:"off=0x11, siz=0x8"`
	Generation    Generation  `bin:"off=0x19, siz=0x8"`
	binstruct.End `bin:"off=0x21"`
}

func (node *Node) unmarshalInternal(bodyBuf []byte) (int, error) {
	n := 0
	for i := uint32(0); i < node.Head.NumItems; i++ {
		var item KeyPointer
		_n, err := binstruct.Unmarshal(bodyBuf[n:], &item)
		n += _n
		if err != nil {
			return n, fmt.Errorf("item %d: %w", i, err)
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
			return fmt.Errorf("item %d: %w", i, err)
		}
		if copy(bodyBuf[n:], bs) < len(bs) {
			return fmt.Errorf("item %d: not enough space: need at least %d+%d=%d bytes, but only have %d",
				i, n, len(bs), n+len(bs), len(bodyBuf))
		}
		n += len(bs)
	}
	if copy(bodyBuf[n:], node.Padding) < len(node.Padding) {
		return fmt.Errorf("padding: not enough space: need at least %d+%d=%d bytes, but only have %d",
			n, len(node.Padding), n+len(node.Padding), len(bodyBuf))
	}
	return nil
}

// Node: "leaf" ////////////////////////////////////////////////////////////////////////////////////

type Item struct {
	Head ItemHeader
	Body btrfsitem.Item
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
		var item Item

		n, err := binstruct.Unmarshal(bodyBuf[head:], &item.Head)
		head += n
		if err != nil {
			return 0, fmt.Errorf("item %d: head: %w", i, err)
		}
		if head > tail {
			return 0, fmt.Errorf("item %d: head: end_offset=0x%0x is in the body section (offset>0x%0x)",
				i, head, tail)
		}

		dataOff := int(item.Head.DataOffset)
		if dataOff < head {
			return 0, fmt.Errorf("item %d: body: beg_offset=0x%0x is in the head section (offset<0x%0x)",
				i, dataOff, head)
		}
		dataSize := int(item.Head.DataSize)
		if dataOff+dataSize != tail {
			return 0, fmt.Errorf("item %d: body: end_offset=0x%0x is not cur_tail=0x%0x)",
				i, dataOff+dataSize, tail)
		}
		tail = dataOff
		dataBuf := bodyBuf[dataOff : dataOff+dataSize]
		item.Body = btrfsitem.UnmarshalItem(item.Head.Key, dataBuf)

		node.BodyLeaf = append(node.BodyLeaf, item)
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
			return fmt.Errorf("item %d: body: %w", i, err)
		}
		item.Head.DataSize = uint32(len(itemBodyBuf))
		item.Head.DataOffset = uint32(tail - len(itemBodyBuf))
		itemHeadBuf, err := binstruct.Marshal(item.Head)
		if err != nil {
			return fmt.Errorf("item %d: head: %w", i, err)
		}

		if tail-head < len(itemHeadBuf)+len(itemBodyBuf) {
			return fmt.Errorf("item %d: not enough space: need at least (head_len:%d)+(body_len:%d)=%d free bytes, but only have %d",
				i, len(itemHeadBuf), len(itemBodyBuf), len(itemHeadBuf)+len(itemBodyBuf), tail-head)
		}

		copy(bodyBuf[head:], itemHeadBuf)
		head += len(itemHeadBuf)
		tail -= len(itemBodyBuf)
		copy(bodyBuf[tail:], itemBodyBuf)
	}
	if copy(bodyBuf[head:tail], node.Padding) < len(node.Padding) {
		return fmt.Errorf("padding: not enough space: need at least %d free bytes, but only have %d",
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
		freeSpace -= item.Head.DataSize
	}
	return freeSpace
}

// Tie Nodes in to the FS //////////////////////////////////////////////////////////////////////////

var ErrNotANode = errors.New("does not look like a node")

func ReadNode[Addr ~int64](fs util.File[Addr], sb Superblock, addr Addr, laddrCB func(LogicalAddr) error) (*util.Ref[Addr, Node], error) {
	nodeBuf := make([]byte, sb.NodeSize)
	if _, err := fs.ReadAt(nodeBuf, addr); err != nil {
		return nil, err
	}

	// parse (early)

	nodeRef := &util.Ref[Addr, Node]{
		File: fs,
		Addr: addr,
	}
	if _, err := binstruct.Unmarshal(nodeBuf, &nodeRef.Data.Head); err != nil {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%d: %w", addr, err)
	}

	// sanity checking

	if nodeRef.Data.Head.MetadataUUID != sb.EffectiveMetadataUUID() {
		return nil, fmt.Errorf("btrfs.ReadNode: node@%d: %w", addr, ErrNotANode)
	}

	stored := nodeRef.Data.Head.Checksum
	calced := CRC32c(nodeBuf[binstruct.StaticSize(CSum{}):])
	if stored != calced {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%d: looks like a node but is corrupt: checksum mismatch: stored=%s calculated=%s",
			addr, stored, calced)
	}

	if laddrCB != nil {
		if err := laddrCB(nodeRef.Data.Head.Addr); err != nil {
			return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%d: %w", addr, err)
		}
	}

	// parse (main)

	if _, err := nodeRef.Data.UnmarshalBinary(nodeBuf); err != nil {
		return nodeRef, fmt.Errorf("btrfs.ReadNode: node@%d: %w", addr, err)
	}

	// return

	return nodeRef, nil
}

func (fs *FS) ReadNode(addr LogicalAddr) (*util.Ref[LogicalAddr, Node], error) {
	sb, err := fs.Superblock()
	if err != nil {
		return nil, fmt.Errorf("btrfs.FS.ReadNode: %w", err)
	}

	return ReadNode[LogicalAddr](fs, sb.Data, addr, func(claimAddr LogicalAddr) error {
		if claimAddr != addr {
			return fmt.Errorf("read from laddr=%d but claims to be at laddr=%d",
				addr, claimAddr)
		}
		return nil
	})
}

type WalkTreeHandler struct {
	// Callbacks for entire nodes
	PreNode  func(LogicalAddr) error
	Node     func(*util.Ref[LogicalAddr, Node], error) error
	PostNode func(*util.Ref[LogicalAddr, Node]) error
	// Callbacks for items on internal nodes
	PreKeyPointer  func(KeyPointer) error
	PostKeyPointer func(KeyPointer) error
	// Callbacks for items on leaf nodes
	Item func(Key, btrfsitem.Item) error
}

func (fs *FS) WalkTree(nodeAddr LogicalAddr, cbs WalkTreeHandler) error {
	if nodeAddr == 0 {
		return nil
	}
	if cbs.PreNode != nil {
		if err := cbs.PreNode(nodeAddr); err != nil {
			if errors.Is(err, iofs.SkipDir) {
				return nil
			}
			return err
		}
	}
	node, err := fs.ReadNode(nodeAddr)
	if cbs.Node != nil {
		err = cbs.Node(node, err)
	}
	if err != nil {
		if errors.Is(err, iofs.SkipDir) {
			return nil
		}
		return fmt.Errorf("btrfs.FS.WalkTree: %w", err)
	}
	if node != nil {
		for _, item := range node.Data.BodyInternal {
			if cbs.PreKeyPointer != nil {
				if err := cbs.PreKeyPointer(item); err != nil {
					if errors.Is(err, iofs.SkipDir) {
						continue
					}
					return err
				}
			}
			if err := fs.WalkTree(item.BlockPtr, cbs); err != nil {
				return err
			}
			if cbs.PostKeyPointer != nil {
				if err := cbs.PostKeyPointer(item); err != nil {
					if errors.Is(err, iofs.SkipDir) {
						continue
					}
					return err
				}
			}
		}
		for _, item := range node.Data.BodyLeaf {
			if cbs.Item != nil {
				if err := cbs.Item(item.Head.Key, item.Body); err != nil {
					if errors.Is(err, iofs.SkipDir) {
						continue
					}
					return fmt.Errorf("btrfs.FS.WalkTree: callback: %w", err)
				}
			}
		}
	}
	if cbs.PostNode != nil {
		if err := cbs.PostNode(node); err != nil {
			if errors.Is(err, iofs.SkipDir) {
				return nil
			}
			return err
		}
	}
	return nil
}
