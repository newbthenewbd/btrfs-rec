package btrfs

import (
	"fmt"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	. "lukeshu.com/btrfs-tools/pkg/btrfs/btrfstyp"
)

// ScanForNodes mimics btrfs-progs
// cmds/rescue-chunk-recover.c:scan_one_device(), except it doesn't do
// anything but log when it finds a node.
func ScanForNodes(dev *Device, sb Superblock) error {
	devSize, err := dev.Size()
	if err != nil {
		return err
	}

	if sb.NodeSize < sb.SectorSize {
		return fmt.Errorf("node_size(%d) < sector_size(%d)",
			sb.NodeSize, sb.SectorSize)
	}

	nodeBuf := make([]byte, sb.NodeSize)
	for pos := PhysicalAddr(0); pos+PhysicalAddr(sb.SectorSize) < devSize; pos += PhysicalAddr(sb.SectorSize) {
		if inSlice(pos, superblockAddrs) {
			fmt.Printf("sector@%d is a superblock\n", pos)
			continue
		}
		if _, err := dev.ReadAt(nodeBuf, pos); err != nil {
			return fmt.Errorf("sector@%d: %w", pos, err)
		}
		var nodeHeader NodeHeader
		if _, err := binstruct.Unmarshal(nodeBuf, &nodeHeader); err != nil {
			return fmt.Errorf("sector@%d: %w", pos, err)
		}
		if !nodeHeader.MetadataUUID.Equal(sb.EffectiveMetadataUUID()) {
			//fmt.Printf("sector@%d does not look like a node\n", pos)
			continue
		}
		if !nodeHeader.Checksum.Equal(CRC32c(nodeBuf[0x20:])) {
			fmt.Printf("sector@%d looks like a node but is corrupt (checksum doesn't match)\n", pos)
			continue
		}

		fmt.Printf("node@%d: physical_addr=0x%0X logical_addr=0x%0X generation=%d owner=%v level=%d\n",
			pos, pos, nodeHeader.Addr, nodeHeader.Generation, nodeHeader.Owner, nodeHeader.Level)

		pos += PhysicalAddr(sb.NodeSize) - PhysicalAddr(sb.SectorSize)
	}

	return nil
}
