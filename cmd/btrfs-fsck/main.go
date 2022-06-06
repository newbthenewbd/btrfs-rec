package main

import (
	"fmt"
	"os"
	"sort"

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

	fmt.Printf("Pass 1: ... scanning for nodes\n")
	foundNodes := make(map[btrfs.LogicalAddr][]btrfs.PhysicalAddr)
	var lostAndFoundChunks []btrfs.SysChunk
	if err := btrfsmisc.ScanForNodes(fs.Devices[0], superblock.Data, func(nodeRef *util.Ref[btrfs.PhysicalAddr, btrfs.Node], err error) {
		if err != nil {
			fmt.Println(err)
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
					fmt.Printf("Pass 1: node@%d: item %d: error: type is CHUNK_ITEM_KEY, but struct is %T\n",
						nodeRef.Addr, i, item.Body)
					continue
				}
				fmt.Printf("Pass 1: node@%d: item %d: found chunk\n",
					nodeRef.Addr, i)
				lostAndFoundChunks = append(lostAndFoundChunks, btrfs.SysChunk{
					Key:   item.Head.Key,
					Chunk: chunk,
				})
			}
		}
	}); err != nil {
		return err
	}

	fmt.Printf("Pass 1: ... re-inserting lost+found chunks\n")
	if len(lostAndFoundChunks) > 0 {
		panic("TODO")
	}

	fmt.Printf("Pass 1: ... re-constructing stripes for lost+found nodes\n")
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
	fmt.Printf("Pass 1: ... reconstructed stripes: %#v\n", stripes)

	fmt.Printf("\nPass 2: ?????????????????????????\n") ////////////////////////////////////////
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
