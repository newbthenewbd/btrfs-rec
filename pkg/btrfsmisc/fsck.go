package btrfsmisc

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/util"
)

// ScanForNodes mimics btrfs-progs
// cmds/rescue-chunk-recover.c:scan_one_device(), except it doesn't do
// anything but log when it finds a node.
func ScanForNodes(dev *btrfs.Device, sb btrfs.Superblock, fn func(*util.Ref[btrfs.PhysicalAddr, btrfs.Node], error)) error {
	devSize, err := dev.Size()
	if err != nil {
		return err
	}

	if sb.NodeSize < sb.SectorSize {
		return fmt.Errorf("node_size(%d) < sector_size(%d)",
			sb.NodeSize, sb.SectorSize)
	}

	nodeBuf := make([]byte, sb.NodeSize)
	for pos := btrfs.PhysicalAddr(0); pos+btrfs.PhysicalAddr(sb.NodeSize) < devSize; pos += btrfs.PhysicalAddr(sb.SectorSize) {
		if util.InSlice(pos, btrfs.SuperblockAddrs) {
			//fmt.Printf("sector@%d is a superblock\n", pos)
			continue
		}

		// read

		if _, err := dev.ReadAt(nodeBuf, pos); err != nil {
			fn(nil, fmt.Errorf("sector@%d: %w", pos, err))
			continue
		}

		// does it look like a node?

		var nodeHeader btrfs.NodeHeader
		if _, err := binstruct.Unmarshal(nodeBuf, &nodeHeader); err != nil {
			fn(nil, fmt.Errorf("sector@%d: %w", pos, err))
		}
		if nodeHeader.MetadataUUID != sb.EffectiveMetadataUUID() {
			//fmt.Printf("sector@%d does not look like a node\n", pos)
			continue
		}

		// ok, it looks like a node; go ahead and read it as a node

		nodeRef := &util.Ref[btrfs.PhysicalAddr, btrfs.Node]{
			File: dev,
			Addr: pos,
			Data: btrfs.Node{
				Size: sb.NodeSize,
			},
		}
		if _, err := nodeRef.Data.UnmarshalBinary(nodeBuf); err != nil {
			fn(nil, fmt.Errorf("sector@%d: %w", pos, err))
			continue
		}

		// finally, process the node

		if nodeRef.Data.Head.Checksum != btrfs.CRC32c(nodeBuf[0x20:]) {
			fn(nodeRef, fmt.Errorf("sector@%d looks like a node but is corrupt (checksum doesn't match)", pos))
			continue
		}

		fn(nodeRef, nil)

		pos += btrfs.PhysicalAddr(sb.NodeSize) - btrfs.PhysicalAddr(sb.SectorSize)
	}

	return nil
}
