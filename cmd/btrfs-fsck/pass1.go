package main

import (
	"encoding/json"
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"sort"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func pass1(fs *btrfs.FS, superblock *util.Ref[btrfs.PhysicalAddr, btrfs.Superblock]) (map[btrfs.LogicalAddr]struct{}, error) {
	fmt.Printf("\nPass 1: chunk mappings...\n")

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

	fsFoundNodes := make(map[btrfs.LogicalAddr]struct{})
	fsReconstructedChunks := make(map[btrfs.LogicalAddr]struct {
		Size    uint64
		Stripes []btrfsitem.ChunkStripe
	})
	for _, dev := range fs.Devices {
		fmt.Printf("Pass 1: ... dev[%q] scanning for nodes...\n", dev.Name())
		devFoundNodes, devLostAndFoundChunks, err := pass1ScanOneDev(dev, superblock.Data, visitedChunkNodes)
		if err != nil {
			return nil, err
		}

		fmt.Printf("Pass 1: ... dev[%q] re-inserting lost+found chunks\n", dev.Name())
		if len(devLostAndFoundChunks) > 0 {
			panic("TODO")
		}

		fmt.Printf("Pass 1: ... dev[%q] re-constructing stripes for lost+found nodes\n", dev.Name())
		devReconstructedChunks := pass1ReconstructChunksOneDev(fs, dev, devFoundNodes)

		// merge those results in to the total-fs results
		for laddr := range devFoundNodes {
			fsFoundNodes[laddr] = struct{}{}
		}
		for laddr, _chunk := range devReconstructedChunks {
			chunk, ok := fsReconstructedChunks[laddr]
			if !ok {
				chunk.Size = _chunk.Size
			}
			if chunk.Size != _chunk.Size {
				panic("TODO: mismatch")
			}
			chunk.Stripes = append(chunk.Stripes, _chunk.Stripes...)
			fsReconstructedChunks[laddr] = chunk
		}
	}

	fmt.Printf("Pass 1: ... writing re-constructed chunks\n")
	pass1WriteReconstructedChunks(fs, superblock.Data, fsReconstructedChunks)

	return fsFoundNodes, nil
}

func pass1ScanOneDev(
	dev *btrfs.Device,
	superblock btrfs.Superblock,
	visitedChunkNodes map[btrfs.LogicalAddr]struct{},
) (
	foundNodes map[btrfs.LogicalAddr][]btrfs.PhysicalAddr,
	lostAndFoundChunks []btrfs.SysChunk,
	err error,
) {
	type rec struct {
		DevFoundNodes         map[btrfs.LogicalAddr][]btrfs.PhysicalAddr
		DevLostAndFoundChunks []btrfs.SysChunk
	}
	var obj rec

	const jsonFilename = "/home/lukeshu/btrfs/pass1.json"

	bs, err := os.ReadFile(jsonFilename)
	if err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			obj.DevFoundNodes, obj.DevLostAndFoundChunks, err = pass1ScanOneDev_x(dev, superblock, visitedChunkNodes)
			if err != nil {
				panic(err)
			}
			bs, err := json.Marshal(obj)
			if err != nil {
				panic(err)
			}
			if err := os.WriteFile(jsonFilename, bs, 0600); err != nil {
				panic(err)
			}
		}
		return nil, nil, err
	}
	if err := json.Unmarshal(bs, &obj); err != nil {
		return nil, nil, err
	}
	return obj.DevFoundNodes, obj.DevLostAndFoundChunks, nil
}

func pass1ScanOneDev_x(
	dev *btrfs.Device,
	superblock btrfs.Superblock,
	visitedChunkNodes map[btrfs.LogicalAddr]struct{},
) (
	foundNodes map[btrfs.LogicalAddr][]btrfs.PhysicalAddr,
	lostAndFoundChunks []btrfs.SysChunk,
	err error,
) {
	foundNodes = make(map[btrfs.LogicalAddr][]btrfs.PhysicalAddr)

	devSize, _ := dev.Size()
	lastProgress := -1

	err = btrfsmisc.ScanForNodes(dev, superblock, func(nodeRef *util.Ref[btrfs.PhysicalAddr, btrfs.Node], err error) {
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
	})

	return
}

func pass1ReconstructChunksOneDev(
	fs *btrfs.FS,
	dev *btrfs.Device,
	foundNodes map[btrfs.LogicalAddr][]btrfs.PhysicalAddr,
) (
	chunks map[btrfs.LogicalAddr]struct {
		Size    uint64
		Stripes []btrfsitem.ChunkStripe
	},
) {
	superblock, _ := dev.Superblock()

	// find the subset of `foundNodes` that are lost
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

	// sort the keys to that set
	sortedPaddrs := make([]btrfs.PhysicalAddr, 0, len(lostAndFoundNodes))
	for paddr := range lostAndFoundNodes {
		sortedPaddrs = append(sortedPaddrs, paddr)
	}
	sort.Slice(sortedPaddrs, func(i, j int) bool {
		return sortedPaddrs[i] < sortedPaddrs[j]
	})

	// build a list of stripes from that sorted set
	type stripe struct {
		PAddr btrfs.PhysicalAddr
		LAddr btrfs.LogicalAddr
		Size  uint64
	}
	var stripes []*stripe
	for _, paddr := range sortedPaddrs {
		var lastStripe *stripe
		if len(stripes) > 0 {
			lastStripe = stripes[len(stripes)-1]
		}
		if lastStripe != nil && (lastStripe.PAddr+btrfs.PhysicalAddr(lastStripe.Size)) == paddr {
			lastStripe.Size += uint64(superblock.Data.NodeSize)
		} else {
			stripes = append(stripes, &stripe{
				PAddr: paddr,
				LAddr: lostAndFoundNodes[paddr],
				Size:  uint64(superblock.Data.NodeSize),
			})
		}
	}
	//fmt.Printf("Pass 1: ... dev[%q] reconstructed stripes: %#v\n", dev.Name(), stripes)

	// organize those stripes in to chunks
	chunks = make(map[btrfs.LogicalAddr]struct {
		Size    uint64
		Stripes []btrfsitem.ChunkStripe
	})
	for _, stripe := range stripes {
		chunk, ok := chunks[stripe.LAddr]
		if !ok {
			chunk.Size = stripe.Size
		}
		if chunk.Size != stripe.Size {
			panic("TODO: mismatch")
		}
		chunk.Stripes = append(chunk.Stripes, btrfsitem.ChunkStripe{
			DeviceID:   superblock.Data.DevItem.DeviceID,
			DeviceUUID: superblock.Data.DevItem.DevUUID,
			Offset:     stripe.PAddr,
		})
		chunks[stripe.LAddr] = chunk
	}

	return chunks
}

func pass1WriteReconstructedChunks(
	fs *btrfs.FS,
	superblock btrfs.Superblock,
	fsReconstructedChunks map[btrfs.LogicalAddr]struct {
		Size    uint64
		Stripes []btrfsitem.ChunkStripe
	},
) {
	// FIXME(lukeshu): OK, so this just assumes that all the
	// reconstructed stripes fit in one node, and that we can just
	// store that node at the root node of the chunk tree.  This
	// isn't true in general, but it's true of my particular
	// filesystem.
	reconstructedNode := &util.Ref[btrfs.LogicalAddr, btrfs.Node]{
		File: fs,
		Addr: superblock.ChunkTree,
		Data: btrfs.Node{
			Size: superblock.NodeSize,
			Head: btrfs.NodeHeader{
				MetadataUUID: superblock.EffectiveMetadataUUID(),
				Addr:         superblock.ChunkTree,
				Flags:        btrfs.NodeWritten,
				//BackrefRef: ???,
				//ChunkTreeUUID: ???,
				Generation: superblock.ChunkRootGeneration,
				Owner:      btrfs.CHUNK_TREE_OBJECTID,
				Level:      0,
			},
		},
	}

	for _, dev := range fs.Devices {
		superblock, _ := dev.Superblock()
		reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
			Head: btrfs.ItemHeader{
				Key: btrfs.Key{
					ObjectID: btrfs.DEV_ITEMS_OBJECTID,
					ItemType: btrfsitem.DEV_ITEM_KEY,
					Offset:   1, // ???
				},
			},
			Body: superblock.Data.DevItem,
		})
	}

	sortedLAddrs := make([]btrfs.LogicalAddr, 0, len(fsReconstructedChunks))
	for laddr := range fsReconstructedChunks {
		sortedLAddrs = append(sortedLAddrs, laddr)
	}
	sort.Slice(sortedLAddrs, func(i, j int) bool {
		return sortedLAddrs[i] < sortedLAddrs[j]
	})
	for i, laddr := range sortedLAddrs {
		chunk := fsReconstructedChunks[laddr]
		for j, stripe := range chunk.Stripes {
			fmt.Printf("Pass 1: chunks[%d].stripes[%d] = { laddr=0x%0x => { dev_id=%d, paddr=0x%0x }, size=0x%0x }\n",
				i, j, laddr, stripe.DeviceID, stripe.Offset, chunk.Size)
		}
		reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
			Head: btrfs.ItemHeader{
				Key: btrfs.Key{
					ObjectID: btrfs.FIRST_CHUNK_TREE_OBJECTID,
					ItemType: btrfsitem.CHUNK_ITEM_KEY,
					Offset:   uint64(laddr),
				},
			},
			Body: btrfsitem.Chunk{
				Head: btrfsitem.ChunkHeader{
					Size:           chunk.Size,
					Owner:          btrfs.EXTENT_TREE_OBJECTID,
					StripeLen:      65536, // ???
					Type:           0,     // TODO
					IOOptimalAlign: superblock.DevItem.IOOptimalAlign,
					IOOptimalWidth: superblock.DevItem.IOOptimalWidth,
					IOMinSize:      superblock.DevItem.IOMinSize,
					NumStripes:     uint16(len(chunk.Stripes)),
					SubStripes:     1,
				},
				Stripes: chunk.Stripes,
			},
		})
	}

	var err error
	reconstructedNode.Data.Head.Checksum, err = reconstructedNode.Data.CalculateChecksum()
	if err != nil {
		fmt.Printf("Pass 1: ... new node checksum: error: %v\n", err)
	}

	if err := reconstructedNode.Write(); err != nil {
		fmt.Printf("Pass 1: ... write new node: error: %v\n", err)
	}

	if err := fs.Init(); err != nil {
		fmt.Printf("Pass 1: ... re-init mappings: %v\n", err)
	}
}
