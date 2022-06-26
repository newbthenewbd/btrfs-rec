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

	fmt.Printf("Pass 1: ... walking fs\n")
	visitedNodes := make(map[btrfs.LogicalAddr]struct{})
	btrfsmisc.WalkFS(fs, btrfs.WalkTreeHandler{
		Node: func(path btrfs.WalkTreePath, node *util.Ref[btrfs.LogicalAddr, btrfs.Node], err error) error {
			if err != nil {
				err = fmt.Errorf("%v: %w", path, err)
				fmt.Printf("Pass 1: ... walk fs: error: %v\n", err)
			}
			if node != nil {
				visitedNodes[node.Addr] = struct{}{}
			}
			return err
		},
	}, func(err error) {
		fmt.Printf("Pass 1: ... walk fs: error: %v\n", err)
	})

	fsFoundNodes := make(map[btrfs.LogicalAddr]struct{})
	fsReconstructedChunks := make(map[btrfs.LogicalAddr]struct {
		Size    uint64
		Stripes []btrfsitem.ChunkStripe
	})
	for _, dev := range fs.Devices {
		fmt.Printf("Pass 1: ... dev[%q] scanning for nodes...\n", dev.Name())
		devResult, err := pass1ScanOneDev(dev, superblock.Data)
		if err != nil {
			return nil, err
		}

		fmt.Printf("Pass 1: ... dev[%q] re-inserting lost+found chunks\n", dev.Name())
		if len(devResult.FoundChunks) > 0 {
			panic("TODO")
		}
		// if len(devResult.FoundBlockGroups) > 0 {
		// 	panic("TODO")
		// }
		// if len(devResult.FoundDevExtents) > 0 {
		// 	panic("TODO")
		// }

		fmt.Printf("Pass 1: ... dev[%q] re-constructing stripes for lost+found nodes\n", dev.Name())
		devReconstructedChunks := pass1ReconstructChunksOneDev(fs, dev, devResult.FoundNodes)

		pass1PrintChunks(devReconstructedChunks)
		pass1ProcessBlockGroups(devResult.FoundBlockGroups)
		pass1ProcessDevExtents(devResult.FoundDevExtents)

		// merge those results in to the total-fs results
		for laddr := range devResult.FoundNodes {
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
	//pass1WriteReconstructedChunks(fs, superblock.Data, fsReconstructedChunks)

	return fsFoundNodes, nil
}

type pass1ScanOneDevResult struct {
	FoundNodes       map[btrfs.LogicalAddr][]btrfs.PhysicalAddr
	FoundChunks      []btrfs.SysChunk
	FoundBlockGroups []sysBlockGroup
	FoundDevExtents  []sysDevExtent
}

type sysBlockGroup struct {
	Key btrfs.Key
	BG  btrfsitem.BlockGroup
}

type sysDevExtent struct {
	Key    btrfs.Key
	DevExt btrfsitem.DevExtent
}

func pass1ScanOneDev(dev *btrfs.Device, superblock btrfs.Superblock) (pass1ScanOneDevResult, error) {
	const jsonFilename = "/home/lukeshu/btrfs/pass1v2.json"

	var result pass1ScanOneDevResult
	bs, err := os.ReadFile(jsonFilename)
	if err != nil {
		if errors.Is(err, iofs.ErrNotExist) {
			result, err := pass1ScanOneDev_real(dev, superblock)
			if err != nil {
				panic(err)
			}
			bs, err := json.Marshal(result)
			if err != nil {
				panic(err)
			}
			if err := os.WriteFile(jsonFilename, bs, 0600); err != nil {
				panic(err)
			}
			return result, nil
		}
		return result, err
	}
	if err := json.Unmarshal(bs, &result); err != nil {
		return result, err
	}
	pass1ScanOneDev_printProgress(dev, 100, result)
	return result, nil
}

func pass1ScanOneDev_printProgress(dev *btrfs.Device, pct int, result pass1ScanOneDevResult) {
	fmt.Printf("Pass 1: ... dev[%q] scanned %v%% (found: %v nodes, %v chunks, %v block groups, %v dev extents)\n",
		dev.Name(), pct,
		len(result.FoundNodes),
		len(result.FoundChunks),
		len(result.FoundBlockGroups),
		len(result.FoundDevExtents))
}

func pass1ScanOneDev_real(dev *btrfs.Device, superblock btrfs.Superblock) (pass1ScanOneDevResult, error) {
	result := pass1ScanOneDevResult{
		FoundNodes: make(map[btrfs.LogicalAddr][]btrfs.PhysicalAddr),
	}

	devSize, _ := dev.Size()
	lastProgress := -1

	err := btrfsmisc.ScanForNodes(dev, superblock, func(nodeRef *util.Ref[btrfs.PhysicalAddr, btrfs.Node], err error) {
		if err != nil {
			fmt.Printf("Pass 1: ... dev[%q] error: %v\n", dev.Name(), err)
			return
		}
		result.FoundNodes[nodeRef.Data.Head.Addr] = append(result.FoundNodes[nodeRef.Data.Head.Addr], nodeRef.Addr)
		for i, item := range nodeRef.Data.BodyLeaf {
			switch item.Head.Key.ItemType {
			case btrfsitem.CHUNK_ITEM_KEY:
				chunk, ok := item.Body.(btrfsitem.Chunk)
				if !ok {
					fmt.Printf("Pass 1: ... dev[%q] node@%v: item %v: error: type is CHUNK_ITEM_KEY, but struct is %T\n",
						dev.Name(), nodeRef.Addr, i, item.Body)
					continue
				}
				//fmt.Printf("Pass 1: ... dev[%q] node@%v: item %v: found chunk\n",
				//	dev.Name(), nodeRef.Addr, i)
				result.FoundChunks = append(result.FoundChunks, btrfs.SysChunk{
					Key:   item.Head.Key,
					Chunk: chunk,
				})
			case btrfsitem.BLOCK_GROUP_ITEM_KEY:
				bg, ok := item.Body.(btrfsitem.BlockGroup)
				if !ok {
					fmt.Printf("Pass 1: ... dev[%q] node@%v: item %v: error: type is BLOCK_GROUP_ITEM_KEY, but struct is %T\n",
						dev.Name(), nodeRef.Addr, i, item.Body)
					continue
				}
				//fmt.Printf("Pass 1: ... dev[%q] node@%v: item %v: found block group\n",
				//	dev.Name(), nodeRef.Addr, i)
				result.FoundBlockGroups = append(result.FoundBlockGroups, sysBlockGroup{
					Key: item.Head.Key,
					BG:  bg,
				})
			case btrfsitem.DEV_EXTENT_KEY:
				devext, ok := item.Body.(btrfsitem.DevExtent)
				if !ok {
					fmt.Printf("Pass 1: ... dev[%q] node@%v: item %v: error: type is DEV_EXTENT_KEY, but struct is %T\n",
						dev.Name(), nodeRef.Addr, i, item.Body)
					continue
				}
				//fmt.Printf("Pass 1: ... dev[%q] node@%v: item %v: found dev extent\n",
				//	dev.Name(), nodeRef.Addr, i)
				result.FoundDevExtents = append(result.FoundDevExtents, sysDevExtent{
					Key:    item.Head.Key,
					DevExt: devext,
				})
			}
		}
	}, func(pos btrfs.PhysicalAddr) {
		pct := int(100 * float64(pos) / float64(devSize))
		if pct != lastProgress || pos == devSize {
			pass1ScanOneDev_printProgress(dev, pct, result)
			lastProgress = pct
		}
	})

	return result, err
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
	for laddr, readPAddrs := range foundNodes {
		resolvedPAddrs, _ := fs.Resolve(laddr)
		for _, readPAddr := range readPAddrs {
			if _, ok := resolvedPAddrs[btrfs.QualifiedPhysicalAddr{
				Dev:  superblock.Data.DevItem.DevUUID,
				Addr: readPAddr,
			}]; !ok {
				lostAndFoundNodes[readPAddr] = laddr
			}
		}
	}

	// sort the keys to that set
	sortedPAddrs := make([]btrfs.PhysicalAddr, 0, len(lostAndFoundNodes))
	for paddr := range lostAndFoundNodes {
		sortedPAddrs = append(sortedPAddrs, paddr)
	}
	sort.Slice(sortedPAddrs, func(i, j int) bool {
		return sortedPAddrs[i] < sortedPAddrs[j]
	})

	// build a list of stripes from that sorted set
	type stripe struct {
		PAddr btrfs.PhysicalAddr
		LAddr btrfs.LogicalAddr
		Size  uint64
	}
	var stripes []*stripe
	for _, paddr := range sortedPAddrs {
		laddr := lostAndFoundNodes[paddr]

		var lastStripe *stripe
		if len(stripes) > 0 {
			lastStripe = stripes[len(stripes)-1]
		}
		if lastStripe != nil &&
			paddr == lastStripe.PAddr+btrfs.PhysicalAddr(lastStripe.Size) &&
			laddr == lastStripe.LAddr+btrfs.LogicalAddr(lastStripe.Size) {
			lastStripe.Size += uint64(superblock.Data.NodeSize)
		} else {
			stripes = append(stripes, &stripe{
				PAddr: paddr,
				LAddr: laddr,
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

func pass1PrintChunks(chunks map[btrfs.LogicalAddr]struct {
	Size    uint64
	Stripes []btrfsitem.ChunkStripe
}) {
	laddrs := make([]btrfs.LogicalAddr, 0, len(chunks))
	for laddr := range chunks {
		laddrs = append(laddrs, laddr)
	}
	sort.Slice(laddrs, func(i, j int) bool {
		return laddrs[i] < laddrs[j]
	})
	lprev := btrfs.LogicalAddr(0)
	pprev := btrfs.PhysicalAddr(0)
	for _, laddr := range laddrs {
		ldelta := laddr - lprev
		chunk := chunks[laddr]
		for _, stripe := range chunk.Stripes {
			pdelta := stripe.Offset - pprev
			adj := "mismatch"
			if uint64(pdelta) == uint64(ldelta) {
				adj = "match"
			}
			fmt.Printf("chunkstripe: laddr=%v (+%v) => paddr=%v (+%v) ; size=%v (%s)\n",
				laddr, ldelta,
				stripe.Offset, pdelta,
				btrfs.PhysicalAddr(chunk.Size),
				adj)
			pprev = stripe.Offset + btrfs.PhysicalAddr(chunk.Size)
		}
		lprev = laddr + btrfs.LogicalAddr(chunk.Size)
	}
}

func pass1ProcessBlockGroups(blockgroups []sysBlockGroup) {
	// organize in to a more manageable datastructure
	type groupAttrs struct {
		Size  btrfs.LogicalAddr
		Flags btrfsitem.BlockGroupFlags
	}
	groups := make(map[btrfs.LogicalAddr]groupAttrs)
	for _, bg := range blockgroups {
		laddr := btrfs.LogicalAddr(bg.Key.ObjectID)
		attrs := groupAttrs{
			Size:  btrfs.LogicalAddr(bg.Key.Offset),
			Flags: bg.BG.Flags,
		}
		// If there's a conflict, but they both say the same thing (existing == attrs),
		// then just ignore the dup.
		if existing, conflict := groups[laddr]; conflict && existing != attrs {
			fmt.Printf("error: conflicting blockgroups for laddr=%v\n", laddr)
			continue
		}
		groups[laddr] = attrs
	}

	// sort the keys to that datastructure
	sortedLAddrs := make([]btrfs.LogicalAddr, 0, len(groups))
	for laddr := range groups {
		sortedLAddrs = append(sortedLAddrs, laddr)
	}
	sort.Slice(sortedLAddrs, func(i, j int) bool {
		return sortedLAddrs[i] < sortedLAddrs[j]
	})

	// cluster
	type cluster struct {
		LAddr btrfs.LogicalAddr
		Size  btrfs.LogicalAddr
		Flags btrfsitem.BlockGroupFlags
	}
	var clusters []*cluster
	for _, laddr := range sortedLAddrs {
		attrs := groups[laddr]

		var lastCluster *cluster
		if len(clusters) > 0 {
			lastCluster = clusters[len(clusters)-1]
		}
		if lastCluster != nil && laddr == lastCluster.LAddr+lastCluster.Size && attrs.Flags == lastCluster.Flags {
			lastCluster.Size += attrs.Size
		} else {
			clusters = append(clusters, &cluster{
				LAddr: laddr,
				Size:  attrs.Size,
				Flags: attrs.Flags,
			})
		}
	}

	// print
	prev := btrfs.LogicalAddr(0)
	for _, cluster := range clusters {
		delta := cluster.LAddr - prev
		fmt.Printf("blockgroup cluster: laddr=%v (+%v); size=%v ; flags=%v\n",
			cluster.LAddr, delta, cluster.Size, cluster.Flags)
		prev = cluster.LAddr + cluster.Size
	}
}

func pass1ProcessDevExtents(devextents []sysDevExtent) {
	// organize in to a more manageable datastructure
	type extAttrs struct {
		LAddr btrfs.LogicalAddr
		Size  uint64
	}
	exts := make(map[btrfs.PhysicalAddr]extAttrs)
	for _, de := range devextents {
		paddr := btrfs.PhysicalAddr(de.Key.Offset)
		attrs := extAttrs{
			Size:  de.DevExt.Length,
			LAddr: de.DevExt.ChunkOffset,
		}
		// If there's a conflict, but they both say the same thing (existing == attrs),
		// then just ignore the dup.
		if existing, conflict := exts[paddr]; conflict && existing != attrs {
			fmt.Printf("error: conflicting devextents for paddr=%v\n", paddr)
			continue
		}
		exts[paddr] = attrs
	}

	// sort the keys to that datastructure
	sortedPAddrs := make([]btrfs.PhysicalAddr, 0, len(exts))
	for paddr := range exts {
		sortedPAddrs = append(sortedPAddrs, paddr)
	}
	sort.Slice(sortedPAddrs, func(i, j int) bool {
		return sortedPAddrs[i] < sortedPAddrs[j]
	})

	// cluster
	type stripe struct {
		PAddr btrfs.PhysicalAddr
		LAddr btrfs.LogicalAddr
		Size  uint64
	}
	var stripes []*stripe
	for _, paddr := range sortedPAddrs {
		attrs := exts[paddr]

		var lastStripe *stripe
		if len(stripes) > 0 {
			lastStripe = stripes[len(stripes)-1]
		}
		if lastStripe != nil &&
			paddr == lastStripe.PAddr+btrfs.PhysicalAddr(lastStripe.Size) &&
			attrs.LAddr == lastStripe.LAddr+btrfs.LogicalAddr(lastStripe.Size) {
			lastStripe.Size += attrs.Size
		} else {
			stripes = append(stripes, &stripe{
				PAddr: paddr,
				LAddr: attrs.LAddr,
				Size:  attrs.Size,
			})
		}
	}

	// print
	lprev := btrfs.LogicalAddr(0)
	pprev := btrfs.PhysicalAddr(0)
	for _, stripe := range stripes {
		pdelta := stripe.PAddr - pprev
		ldelta := stripe.LAddr - lprev
		fmt.Printf("devextent cluster: paddr=%v (+%v) => laddr=%v (+%v) ; size=%v\n",
			stripe.PAddr, pdelta,
			stripe.LAddr, ldelta,
			btrfs.PhysicalAddr(stripe.Size))
		pprev = stripe.PAddr + btrfs.PhysicalAddr(stripe.Size)
		lprev = stripe.LAddr + btrfs.LogicalAddr(stripe.Size)
	}
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
			fmt.Printf("Pass 1: chunks[%v].stripes[%v] = { laddr=%v => { dev_id=%v, paddr=%v }, size=%v }\n",
				i, j, laddr, stripe.DeviceID, stripe.Offset, btrfs.LogicalAddr(chunk.Size))
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
