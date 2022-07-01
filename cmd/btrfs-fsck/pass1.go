package main

import (
	"encoding/json"
	"errors"
	"fmt"
	iofs "io/fs"
	"os"
	"sort"

	"golang.org/x/text/message"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func pass1(fs *btrfs.FS, superblock *util.Ref[btrfs.PhysicalAddr, btrfs.Superblock]) (map[btrfs.LogicalAddr]struct{}, error) {
	fmt.Printf("\nPass 1: chunk mappings...\n")

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
	for _, dev := range fs.LV.PhysicalVolumes() {
		fmt.Printf("Pass 1: ... dev[%q] scanning for nodes...\n", dev.Name())
		devResult, err := pass1ScanOneDev(dev, superblock.Data)
		if err != nil {
			return nil, err
		}

		fmt.Printf("Pass 1: ... dev[%q] re-inserting lost+found mappings\n", dev.Name())
		devResult.AddToLV(fs, dev)

		// merge those results in to the total-fs results
		for laddr := range devResult.FoundNodes {
			fsFoundNodes[laddr] = struct{}{}
		}
	}

	fmt.Printf("Pass 1: ... logical address space:\n")
	pass1PrintLogicalSpace(fs)
	fmt.Printf("Pass 1: ... physical address space:\n")
	pass1PrintPhysicalSpace(fs)

	fmt.Printf("Pass 1: ... writing re-constructed chunks\n")
	pass1WriteReconstructedChunks(fs)

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

func (found pass1ScanOneDevResult) AddToLV(fs *btrfs.FS, dev *btrfs.Device) {
	sb, _ := dev.Superblock()

	total := len(found.FoundChunks) + len(found.FoundDevExtents)
	for _, paddrs := range found.FoundNodes {
		total += len(paddrs)
	}
	lastProgress := -1
	done := 0
	printProgress := func() {
		pct := int(100 * float64(done) / float64(total))
		if pct != lastProgress || done == total {
			fmt.Printf("Pass 1: ... added %v%% of the mappings (%v/%v=>%v)\n",
				pct, done, total, len(fs.LV.Mappings()))
			lastProgress = pct
		}
	}
	printProgress()

	for _, chunk := range found.FoundChunks {
		for _, mapping := range chunk.Chunk.Mappings(chunk.Key) {
			if err := fs.LV.AddMapping(mapping); err != nil {
				fmt.Printf("Pass 1: ... error: adding chunk: %v\n", err)
			}
			done++
			printProgress()
		}
	}

	for _, ext := range found.FoundDevExtents {
		if err := fs.LV.AddMapping(ext.DevExt.Mapping(ext.Key)); err != nil {
			fmt.Printf("Pass 1: ... error: adding devext: %v\n", err)
		}
		done++
		printProgress()
	}

	// Do the nodes last to avoid bloating the mappings table too
	// much. (Because nodes are numerous and small, while the
	// others are few and large; so it is likely that many of the
	// nodes will be subsumed by other things.)
	//
	// Sort them so that progress numbers are predictable.
	for _, laddr := range util.SortedMapKeys(found.FoundNodes) {
		for _, paddr := range found.FoundNodes[laddr] {
			if err := fs.LV.AddMapping(btrfsvol.Mapping{
				LAddr: laddr,
				PAddr: btrfsvol.QualifiedPhysicalAddr{
					Dev:  sb.Data.DevItem.DevID,
					Addr: paddr,
				},
				Size:       btrfsvol.AddrDelta(sb.Data.NodeSize),
				SizeLocked: false,
				Flags:      nil,
			}); err != nil {
				fmt.Printf("Pass 1: ... error: adding node ident: %v\n", err)
			}
			done++
			printProgress()
		}
	}

	// Use block groups to add missing flags (and as a hint to
	// combine node entries).
	//
	// First dedup them, because they change for allocations and
	// CoW means that they'll bounce around a lot, so you likely
	// have oodles of duplicates?
	type blockgroup struct {
		LAddr btrfsvol.LogicalAddr
		Size  btrfsvol.AddrDelta
		Flags btrfsvol.BlockGroupFlags
	}
	bgsSet := make(map[blockgroup]struct{})
	for _, bg := range found.FoundBlockGroups {
		bgsSet[blockgroup{
			LAddr: btrfsvol.LogicalAddr(bg.Key.ObjectID),
			Size:  btrfsvol.AddrDelta(bg.Key.Offset),
			Flags: bg.BG.Flags,
		}] = struct{}{}
	}
	bgsOrdered := util.MapKeys(bgsSet)
	sort.Slice(bgsOrdered, func(i, j int) bool {
		return bgsOrdered[i].LAddr < bgsOrdered[j].LAddr
	})
	for _, bg := range bgsOrdered {
		otherLAddr, otherPAddr := fs.LV.ResolveAny(bg.LAddr, bg.Size)
		if otherLAddr < 0 || otherPAddr.Addr < 0 {
			fmt.Printf("Pass 1: ... error: could not pair blockgroup laddr=%v (size=%v flags=%v) with a mapping\n",
				bg.LAddr, bg.Size, bg.Flags)
			continue
		}

		offsetWithinChunk := otherLAddr.Sub(bg.LAddr)
		flags := bg.Flags
		mapping := btrfsvol.Mapping{
			LAddr: bg.LAddr,
			PAddr: btrfsvol.QualifiedPhysicalAddr{
				Dev:  otherPAddr.Dev,
				Addr: otherPAddr.Addr.Add(-offsetWithinChunk),
			},
			Size:       bg.Size,
			SizeLocked: true,
			Flags:      &flags,
		}
		if err := fs.LV.AddMapping(mapping); err != nil {
			fmt.Printf("Pass 1: ... error: adding flags from blockgroup: %v\n", err)
		}
	}
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

func pass1PrintLogicalSpace(fs *btrfs.FS) {
	mappings := fs.LV.Mappings()
	var prevBeg, prevEnd btrfsvol.LogicalAddr
	var sumHole, sumChunk btrfsvol.AddrDelta
	for _, mapping := range mappings {
		if mapping.LAddr > prevEnd {
			size := mapping.LAddr.Sub(prevEnd)
			fmt.Printf("logical_hole laddr=%v size=%v\n", prevEnd, size)
			sumHole += size
		}
		if mapping.LAddr != prevBeg {
			if mapping.Flags == nil {
				fmt.Printf("chunk laddr=%v size=%v flags=(missing)\n",
					mapping.LAddr, mapping.Size)
			} else {
				fmt.Printf("chunk laddr=%v size=%v flags=%v\n",
					mapping.LAddr, mapping.Size, *mapping.Flags)
			}
		}
		fmt.Printf("\tstripe dev_id=%v paddr=%v\n",
			mapping.PAddr.Dev, mapping.PAddr.Addr)
		sumChunk += mapping.Size
		prevBeg = mapping.LAddr
		prevEnd = mapping.LAddr.Add(mapping.Size)
	}
	p := message.NewPrinter(message.MatchLanguage("en"))
	p.Printf("total logical holes      = %v (%d)\n", sumHole, int64(sumHole))
	p.Printf("total logical chunks     = %v (%d)\n", sumChunk, int64(sumChunk))
	p.Printf("total logical addr space = %v (%d)\n", prevEnd, int64(prevEnd))
}

func pass1PrintPhysicalSpace(fs *btrfs.FS) {
	mappings := fs.LV.Mappings()
	sort.Slice(mappings, func(i, j int) bool {
		return mappings[i].PAddr.Cmp(mappings[j].PAddr) < 0
	})

	var prevDev btrfsvol.DeviceID = 0
	var prevEnd btrfsvol.PhysicalAddr
	var sumHole, sumExt btrfsvol.AddrDelta
	for _, mapping := range mappings {
		if mapping.PAddr.Dev != prevDev {
			prevDev = mapping.PAddr.Dev
			prevEnd = 0
		}
		if mapping.PAddr.Addr > prevEnd {
			size := mapping.PAddr.Addr.Sub(prevEnd)
			fmt.Printf("physical_hole paddr=%v size=%v\n", prevEnd, size)
			sumHole += size
		}
		fmt.Printf("devext dev=%v paddr=%v size=%v laddr=%v\n",
			mapping.PAddr.Dev, mapping.PAddr.Addr, mapping.Size, mapping.LAddr)
		sumExt += mapping.Size
		prevEnd = mapping.PAddr.Addr.Add(mapping.Size)
	}
	p := message.NewPrinter(message.MatchLanguage("en"))
	p.Printf("total physical holes      = %v (%d)\n", sumHole, int64(sumHole))
	p.Printf("total physical extents    = %v (%d)\n", sumExt, int64(sumExt))
	p.Printf("total physical addr space = %v (%d)\n", prevEnd, int64(prevEnd))
}

func pass1WriteReconstructedChunks(fs *btrfs.FS) {
	sbRef, _ := fs.Superblock()
	superblock := sbRef.Data

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

	for _, dev := range fs.LV.PhysicalVolumes() {
		superblock, _ := dev.Superblock()
		reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
			Head: btrfs.ItemHeader{
				Key: btrfs.Key{
					ObjectID: btrfs.DEV_ITEMS_OBJECTID,
					ItemType: btrfsitem.DEV_ITEM_KEY,
					Offset:   uint64(superblock.Data.DevItem.DevID),
				},
			},
			Body: superblock.Data.DevItem,
		})
	}

	for _, mapping := range fs.LV.Mappings() {
		chunkIdx := len(reconstructedNode.Data.BodyLeaf) - 1
		if len(reconstructedNode.Data.BodyLeaf) == 0 || reconstructedNode.Data.BodyLeaf[chunkIdx].Head.Key.Offset != uint64(mapping.LAddr) {
			reconstructedNode.Data.BodyLeaf = append(reconstructedNode.Data.BodyLeaf, btrfs.Item{
				Head: btrfs.ItemHeader{
					Key: btrfs.Key{
						ObjectID: btrfs.FIRST_CHUNK_TREE_OBJECTID,
						ItemType: btrfsitem.CHUNK_ITEM_KEY,
						Offset:   uint64(mapping.LAddr),
					},
				},
				Body: btrfsitem.Chunk{
					Head: btrfsitem.ChunkHeader{
						Size:           mapping.Size,
						Owner:          btrfs.EXTENT_TREE_OBJECTID,
						StripeLen:      65536, // ???
						Type:           *mapping.Flags,
						IOOptimalAlign: superblock.DevItem.IOOptimalAlign,
						IOOptimalWidth: superblock.DevItem.IOOptimalWidth,
						IOMinSize:      superblock.DevItem.IOMinSize,
						SubStripes:     1,
					},
				},
			})
			chunkIdx++
		}
		dev := fs.LV.PhysicalVolumes()[mapping.PAddr.Dev]
		devSB, _ := dev.Superblock()
		chunkBody := reconstructedNode.Data.BodyLeaf[chunkIdx].Body.(btrfsitem.Chunk)
		chunkBody.Stripes = append(chunkBody.Stripes, btrfsitem.ChunkStripe{
			DeviceID:   mapping.PAddr.Dev,
			Offset:     mapping.PAddr.Addr,
			DeviceUUID: devSB.Data.DevItem.DevUUID,
		})
		reconstructedNode.Data.BodyLeaf[chunkIdx].Body = chunkBody
	}

	var err error
	reconstructedNode.Data.Head.Checksum, err = reconstructedNode.Data.CalculateChecksum()
	if err != nil {
		fmt.Printf("Pass 1: ... new node checksum: error: %v\n", err)
	}

	if err := reconstructedNode.Write(); err != nil {
		fmt.Printf("Pass 1: ... write new node: error: %v\n", err)
	}

	if err := fs.ReInit(); err != nil {
		fmt.Printf("Pass 1: ... re-init mappings: %v\n", err)
	}

	sbs, _ := fs.Superblocks()
	for i, sb := range sbs {
		sb.Data.ChunkLevel = reconstructedNode.Data.Head.Level
		sb.Data.Checksum, err = sb.Data.CalculateChecksum()
		if err != nil {
			fmt.Printf("Pass 1: ... calculate superblock %d checksum: %v\n", i, err)
		}
		if err := sb.Write(); err != nil {
			fmt.Printf("Pass 1: ... write superblock %d: %v\n", i, err)
		}
	}
}
