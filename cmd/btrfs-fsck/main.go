package main

import (
	"fmt"
	"os"
	"sort"

	"lukeshu.com/btrfs-tools/pkg/binstruct"
	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func main() {
	if err := Main(os.Args[1]); err != nil {
		fmt.Fprintf(os.Stderr, "%s: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilename string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fh, err := os.Open(imgfilename)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fh.Close())
	}()
	fs := &btrfs.FS{
		Devices: []*btrfs.Device{
			{
				File: fh,
			},
		},
	}

	fmt.Printf("\nPass 0: superblocks...\n") ///////////////////////////////////////////////////

	superblock, err := fs.Superblock()
	if err != nil {
		return fmt.Errorf("superblock: %w", err)
	}

	fmt.Printf("\nPass 1: chunk mappings...\n") ////////////////////////////////////////////////

	fmt.Printf("Pass 1: ... initializing chunk mappings\n")
	if err := fs.Init(); err != nil {
		fmt.Printf("Pass 1: ... init chunk tree: error: %v\n", err)
	}

	fmt.Printf("Pass 1: ... walking chunk tree\n")
	visitedChunkNodes := make(map[btrfs.LogicalAddr]struct{})
	if err := fs.WalkTree(superblock.Data.ChunkTree, btrfs.WalkTreeHandler{
		Node: func(node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
			if err != nil {
				fmt.Printf("Pass 1: ... walk chunk tree: error: %v\n", err)
			}
			if node != nil {
				visitedChunkNodes[node.Addr] = struct{}{}
			}
			return err
		},
	}); err != nil {
		fmt.Printf("Pass 1: ... walk chunk tree: error: %v\n", err)
	}

	type reconstructedStripe struct {
		Size uint64
		Dev  *btrfs.Device
		Addr btrfs.PhysicalAddr
	}
	reconstructedChunks := make(map[btrfs.LogicalAddr][]reconstructedStripe)
	for _, dev := range fs.Devices {
		fmt.Printf("Pass 1: ... dev[%q] scanning for nodes...\n", dev.Name())
		superblock, _ := dev.Superblock()
		foundNodes := make(map[btrfs.LogicalAddr][]btrfs.PhysicalAddr)
		var lostAndFoundChunks []btrfs.SysChunk
		devSize, _ := dev.Size()
		lastProgress := -1
		if err := btrfsmisc.ScanForNodes(dev, superblock.Data, func(nodeRef *util.Ref[btrfs.PhysicalAddr, btrfs.Node], err error) {
			if err != nil {
				fmt.Printf("Pass 1: ... dev[%q] error: %v\n", dev.Name(), err)
				return
			}
			foundNodes[nodeRef.Data.Head.Addr] = append(foundNodes[nodeRef.Data.Head.Addr], nodeRef.Addr)
			_, alreadyVisited := visitedChunkNodes[nodeRef.Data.Head.Addr]
			if nodeRef.Data.Head.Owner == btrfs.CHUNK_TREE_OBJECTID && !alreadyVisited {
				for i, item := range nodeRef.Data.BodyLeaf {
					if item.Head.Key.ItemType != btrfsitem.CHUNK_ITEM_KEY {
						continue
					}
					chunk, ok := item.Body.(btrfsitem.Chunk)
					if !ok {
						fmt.Printf("Pass 1: ... dev[%q] node@%d: item %d: error: type is CHUNK_ITEM_KEY, but struct is %T\n",
							dev.Name(), nodeRef.Addr, i, item.Body)
						continue
					}
					fmt.Printf("Pass 1: ... dev[%q] node@%d: item %d: found chunk\n",
						dev.Name(), nodeRef.Addr, i)
					lostAndFoundChunks = append(lostAndFoundChunks, btrfs.SysChunk{
						Key:   item.Head.Key,
						Chunk: chunk,
					})
				}
			}
		}, func(pos btrfs.PhysicalAddr) {
			pct := int(100 * float64(pos) / float64(devSize))
			if pct != lastProgress || pos == devSize {
				fmt.Printf("Pass 1: ... dev[%q] scanned %v%% (found %d nodes)\n",
					dev.Name(), pct, len(foundNodes))
				lastProgress = pct
			}
		}); err != nil {
			return err
		}

		fmt.Printf("Pass 1: ... dev[%q] re-inserting lost+found chunks\n", dev.Name())
		if len(lostAndFoundChunks) > 0 {
			panic("TODO")
		}

		fmt.Printf("Pass 1: ... dev[%q] re-constructing stripes for lost+found nodes\n", dev.Name())
		lostAndFoundNodes := make(map[btrfs.PhysicalAddr]btrfs.LogicalAddr)
		for laddr, readPaddrs := range foundNodes {
			resolvedPaddrs, _ := fs.Resolve(laddr)
			for _, readPaddr := range readPaddrs {
				if _, ok := resolvedPaddrs[btrfs.QualifiedPhysicalAddr{
					Dev:  superblock.Data.DevItem.DevUUID,
					Addr: readPaddr,
				}]; !ok {
					lostAndFoundNodes[readPaddr] = laddr
				}
			}
		}
		sortedPaddrs := make([]btrfs.PhysicalAddr, 0, len(lostAndFoundNodes))
		for paddr := range lostAndFoundNodes {
			sortedPaddrs = append(sortedPaddrs, paddr)
		}
		sort.Slice(sortedPaddrs, func(i, j int) bool {
			return sortedPaddrs[i] < sortedPaddrs[j]
		})
		type stripe struct {
			PAddr btrfs.PhysicalAddr
			LAddr btrfs.LogicalAddr
			Size  uint64
		}
		var stripes []stripe
		for _, paddr := range sortedPaddrs {
			var lastStripe *stripe
			if len(stripes) > 0 {
				lastStripe = &stripes[len(stripes)-1]
			}
			if lastStripe != nil && (lastStripe.PAddr+btrfs.PhysicalAddr(lastStripe.Size)) == paddr {
				lastStripe.Size += uint64(superblock.Data.NodeSize)
			} else {
				stripes = append(stripes, stripe{
					PAddr: paddr,
					LAddr: lostAndFoundNodes[paddr],
					Size:  uint64(superblock.Data.NodeSize),
				})
			}
		}
		//fmt.Printf("Pass 1: ... dev[%q] reconstructed stripes: %#v\n", dev.Name(), stripes)
		for _, stripe := range stripes {
			reconstructedChunks[stripe.LAddr] = append(reconstructedChunks[stripe.LAddr], reconstructedStripe{
				Size: stripe.Size,
				Dev:  dev,
				Addr: stripe.PAddr,
			})
		}
	}
	// FIXME(lukeshu): OK, so this just assumes that all the
	// reconstructed stripes fit in one node, and that we can just
	// store that node at the root node of the chunk tree.  This
	// isn't true in general, but it's true of my particular
	// filesystem.
	reconstructedNode := &util.Ref[btrfs.LogicalAddr, btrfs.Node]{
		File: fs,
		Addr: superblock.Data.ChunkTree,
		Data: btrfs.Node{
			Size: superblock.Data.NodeSize,
			Head: btrfs.NodeHeader{
				MetadataUUID: superblock.Data.EffectiveMetadataUUID(),
				Addr:         superblock.Data.ChunkTree,
				Flags:        btrfs.NodeWritten,
				//BackrefRef: ???,
				//ChunkTreeUUID: ???,
				Generation: superblock.Data.ChunkRootGeneration,
				Owner:      btrfs.CHUNK_TREE_OBJECTID,
				Level:      0,
			},
		},
	}
	itemOff := uint32(uint64(superblock.Data.NodeSize) - uint64(binstruct.StaticSize(btrfs.ItemHeader{})))
	for _, dev := range fs.Devices {
		itemSize := uint32(binstruct.StaticSize(btrfsitem.Dev{}))
		itemOff -= itemSize
		superblock, _ := dev.Superblock()
		reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
			Head: btrfs.ItemHeader{
				Key: btrfs.Key{
					ObjectID: btrfs.DEV_ITEMS_OBJECTID,
					ItemType: btrfsitem.DEV_ITEM_KEY,
					Offset:   1, // ???
				},
				DataOffset: itemOff,
				DataSize:   itemSize,
			},
			Body: superblock.Data.DevItem,
		})
	}
	for laddr, stripes := range reconstructedChunks {
		stripeSize := stripes[0].Size
		for _, stripe := range stripes {
			if stripe.Size != stripeSize {
				panic("TODO: mismatch")
			}
		}
		itemSize := uint32(binstruct.StaticSize(btrfsitem.ChunkHeader{}) + (len(stripes) * binstruct.StaticSize(btrfsitem.ChunkStripe{})))
		itemOff -= itemSize
		chunkStripes := make([]btrfsitem.ChunkStripe, len(stripes))
		for i := range stripes {
			superblock, _ := stripes[i].Dev.Superblock()
			chunkStripes[i] = btrfsitem.ChunkStripe{
				DeviceID:   superblock.Data.DevItem.DeviceID,
				DeviceUUID: superblock.Data.DevItem.DevUUID,
				Offset:     stripes[i].Addr,
			}
		}
		reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
			Head: btrfs.ItemHeader{
				Key: btrfs.Key{
					ObjectID: btrfs.FIRST_CHUNK_TREE_OBJECTID,
					ItemType: btrfsitem.CHUNK_ITEM_KEY,
					Offset:   uint64(laddr),
				},
				DataOffset: itemOff,
				DataSize:   itemSize,
			},
			Body: btrfsitem.Chunk{
				Head: btrfsitem.ChunkHeader{
					Size:           stripeSize,
					Owner:          btrfs.EXTENT_TREE_OBJECTID,
					StripeLen:      65536, // ???
					Type:           0,     // TODO
					IOOptimalAlign: superblock.Data.DevItem.IOOptimalAlign,
					IOOptimalWidth: superblock.Data.DevItem.IOOptimalWidth,
					IOMinSize:      superblock.Data.DevItem.IOMinSize,
					NumStripes:     uint16(len(stripes)),
					SubStripes:     1,
				},
				Stripes: chunkStripes,
			},
		})
	}
	reconstructedNode.Data.Head.NumItems = uint32(len(reconstructedNode.Data.BodyLeaf))
	reconstructedNode.Data.Head.Checksum, err = reconstructedNode.Data.CalculateChecksum()
	if err != nil {
		fmt.Printf("Pass 1: ... new node checksum: error: %v\n", err)
	}
	if err := reconstructedNode.Write(); err != nil {
		fmt.Printf("Pass 1: ... write new node: error: %v\n", err)
	}

	fmt.Printf("\nPass 2: orphaned nodes\n") ///////////////////////////////////////////////////
	/*

		fmt.Printf("node@%d: physical_addr=0x%0X logical_addr=0x%0X generation=%d owner=%v level=%d\n",
			nodeRef.Addr,
			nodeRef.Addr, nodeRef.Data.Head.Addr,
			nodeRef.Data.Head.Generation, nodeRef.Data.Head.Owner, nodeRef.Data.Head.Level)
		srcPaddr := btrfs.QualifiedPhysicalAddr{
			Dev:  superblock.Data.DevItem.DevUUID,
			Addr: nodeRef.Addr,
		}
		resPaddrs, _ := fs.Resolve(nodeRef.Data.Head.Addr)
		if len(resPaddrs) == 0 {
			fmt.Printf("node@%d: logical_addr=0x%0X is not mapped\n",
				nodeRef.Addr, nodeRef.Data.Head.Addr)
		} else if _, ok := resPaddrs[srcPaddr]; !ok {
			fmt.Printf("node@%d: logical_addr=0x%0X maps to %v, not %v\n",
				nodeRef.Addr, nodeRef.Data.Head.Addr, resPaddrs, srcPaddr)
		}
	*/
	return nil
}
