package btrfs

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
)

type Img struct {
	*os.File
}

func (img *Img) Size() (int64, error) {
	fi, err := img.Stat()
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

type Ref[T any] struct {
	img  *Img
	addr int64
	Data T
}

func (r *Ref[T]) Read() error {
	size, err := binstruct.Size(r.Data)
	if err != nil {
		return err
	}
	buf := make([]byte, size)
	if _, err := r.img.ReadAt(buf, r.addr); err != nil {
		return err
	}
	return binstruct.Unmarshal(buf, &r.Data)
}

var superblockAddrs = []int64{
	0x00_0001_0000, // 64KiB
	0x00_0400_0000, // 64MiB
	0x40_0000_0000, // 256GiB
}

func (img *Img) Superblocks() ([]Ref[Superblock], error) {
	const superblockSize = 0x1000

	sz, err := img.Size()
	if err != nil {
		return nil, err
	}

	var ret []Ref[Superblock]
	for i, addr := range superblockAddrs {
		if addr+superblockSize <= sz {
			superblock := Ref[Superblock]{
				img:  img,
				addr: addr,
			}
			if err := superblock.Read(); err != nil {
				return nil, fmt.Errorf("superblock %d: %w", i, err)
			}
			ret = append(ret, superblock)
		}
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("no superblocks")
	}
	return ret, nil
}

// ScanForNodes mimics btrfs-progs
// cmds/rescue-chunk-recover.c:scan_one_device(), except it doesn't do
// anything but log when it finds a node.
func (img *Img) ScanForNodes(sb Superblock) error {
	devSize, err := img.Size()
	if err != nil {
		return err
	}

	if sb.NodeSize < sb.SectorSize {
		return fmt.Errorf("node_size(%d) < sector_size(%d)",
			sb.NodeSize, sb.SectorSize)
	}

	nodeBuf := make([]byte, sb.NodeSize)
	for pos := int64(0); pos < devSize; pos += int64(sb.SectorSize) {
		if inSlice(pos, superblockAddrs) {
			fmt.Printf("sector@%d is a superblock\n", pos)
			continue
		}
		if _, err := img.ReadAt(nodeBuf, pos); err != nil {
			return fmt.Errorf("sector@%d: %w", pos, err)
		}
		var nodeHeader NodeHeader
		if err := binstruct.Unmarshal(nodeBuf, &nodeHeader); err != nil {
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

		fmt.Printf("node@%d: physical_addr=0x%0X logical_addr=0x%0X generation=%d owner_tree=%v level=%d\n",
			pos, pos, nodeHeader.Addr, nodeHeader.Generation, nodeHeader.OwnerTree, nodeHeader.Level)

		pos += int64(sb.NodeSize) - int64(sb.SectorSize)
	}

	return nil
}
