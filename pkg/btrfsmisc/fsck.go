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

		// parse (early)

		nodeRef := &util.Ref[btrfs.PhysicalAddr, btrfs.Node]{
			File: dev,
			Addr: pos,
			Data: btrfs.Node{
				Size: sb.NodeSize,
			},
		}
		if _, err := binstruct.Unmarshal(nodeBuf, &nodeRef.Data.Head); err != nil {
			fn(nil, fmt.Errorf("sector@%d: %w", pos, err))
		}

		// sanity checking

		if nodeRef.Data.Head.MetadataUUID != sb.EffectiveMetadataUUID() {
			//fmt.Printf("sector@%d does not look like a node\n", pos)
			continue
		}

		stored := nodeRef.Data.Head.Checksum
		calced := btrfs.CRC32c(nodeBuf[binstruct.StaticSize(btrfs.CSum{}):])
		if stored != calced {
			fn(nodeRef, fmt.Errorf("sector@%d: looks like a node but is corrupt: checksum doesn't match: stored=%s calculated=%s",
				pos, stored, calced))
			continue
		}

		// parse (main)

		if _, err := nodeRef.Data.UnmarshalBinary(nodeBuf); err != nil {
			fn(nil, fmt.Errorf("sector@%d: %w", pos, err))
			continue
		}

		// finally, process the node

		fn(nodeRef, nil)

		pos += btrfs.PhysicalAddr(sb.NodeSize) - btrfs.PhysicalAddr(sb.SectorSize)
	}

	return nil
}
