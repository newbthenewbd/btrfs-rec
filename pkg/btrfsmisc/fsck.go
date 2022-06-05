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
func ScanForNodes(dev *btrfs.Device, sb btrfs.Superblock) error {
	devSize, err := dev.Size()
	if err != nil {
		return err
	}

	if sb.NodeSize < sb.SectorSize {
		return fmt.Errorf("node_size(%d) < sector_size(%d)",
			sb.NodeSize, sb.SectorSize)
	}

	nodeBuf := make([]byte, sb.NodeSize)
	for pos := btrfs.PhysicalAddr(0); pos+btrfs.PhysicalAddr(sb.SectorSize) < devSize; pos += btrfs.PhysicalAddr(sb.SectorSize) {
		if util.InSlice(pos, btrfs.SuperblockAddrs) {
			fmt.Printf("sector@%d is a superblock\n", pos)
			continue
		}
		if _, err := dev.ReadAt(nodeBuf, pos); err != nil {
			return fmt.Errorf("sector@%d: %w", pos, err)
		}
		var nodeHeader btrfs.NodeHeader
		if _, err := binstruct.Unmarshal(nodeBuf, &nodeHeader); err != nil {
			return fmt.Errorf("sector@%d: %w", pos, err)
		}
		if !nodeHeader.MetadataUUID.Equal(sb.EffectiveMetadataUUID()) {
			//fmt.Printf("sector@%d does not look like a node\n", pos)
			continue
		}
		if !nodeHeader.Checksum.Equal(btrfs.CRC32c(nodeBuf[0x20:])) {
			fmt.Printf("sector@%d looks like a node but is corrupt (checksum doesn't match)\n", pos)
			continue
		}

		fmt.Printf("node@%d: physical_addr=0x%0X logical_addr=0x%0X generation=%d owner=%v level=%d\n",
			pos, pos, nodeHeader.Addr, nodeHeader.Generation, nodeHeader.Owner, nodeHeader.Level)

		pos += btrfs.PhysicalAddr(sb.NodeSize) - btrfs.PhysicalAddr(sb.SectorSize)
	}

	return nil
}
