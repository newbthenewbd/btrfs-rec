package btrfsmisc

import (
	"fmt"
	"os"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsitem"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfssum"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

// PrintTree mimics btrfs-progs
// kernel-shared/print-tree.c:btrfs_print_tree() and
// kernel-shared/print-tree.c:btrfs_print_leaf()
func PrintTree(fs *btrfs.FS, root btrfsvol.LogicalAddr) error {
	return fs.TreeWalk(root, btrfs.TreeWalkHandler{
		Node: func(path btrfs.TreePath, nodeRef *util.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
			if err != nil {
				fmt.Fprintf(os.Stderr, "error: %v: %v\n", path, err)
			}
			if nodeRef != nil {
				printHeaderInfo(nodeRef.Data)
			}
			return nil
		},
		PreKeyPointer: func(_ btrfs.TreePath, item btrfs.KeyPointer) error {
			fmt.Printf("\t%v block %v gen %v\n",
				FmtKey(item.Key),
				item.BlockPtr,
				item.Generation)
			return nil
		},
		Item: func(path btrfs.TreePath, item btrfs.Item) error {
			i := path[len(path)-1].ItemIdx
			fmt.Printf("\titem %v %v itemoff %v itemsize %v\n",
				i,
				FmtKey(item.Head.Key),
				item.Head.DataOffset,
				item.Head.DataSize)
			switch body := item.Body.(type) {
			case btrfsitem.FreeSpaceHeader:
				fmt.Printf("\t\tlocation %v\n", FmtKey(body.Location))
				fmt.Printf("\t\tcache generation %v entries %v bitmaps %v\n",
					body.Generation, body.NumEntries, body.NumBitmaps)
			case btrfsitem.Inode:
				fmt.Printf(""+
					"\t\tgeneration %v transid %v size %v nbytes %v\n"+
					"\t\tblock group %v mode %o links %v uid %v gid %v rdev %v\n"+
					"\t\tsequence %v flags %v\n",
					body.Generation, body.TransID, body.Size, body.NumBytes,
					body.BlockGroup, body.Mode, body.NLink, body.UID, body.GID, body.RDev,
					body.Sequence, body.Flags)
				fmt.Printf("\t\tatime %v\n", fmtTime(body.ATime))
				fmt.Printf("\t\tctime %v\n", fmtTime(body.CTime))
				fmt.Printf("\t\tmtime %v\n", fmtTime(body.MTime))
				fmt.Printf("\t\totime %v\n", fmtTime(body.OTime))
			case btrfsitem.InodeRefList:
				for _, ref := range body {
					fmt.Printf("\t\tindex %v namelen %v name: %s\n",
						ref.Index, ref.NameLen, ref.Name)
				}
			//case btrfsitem.INODE_EXTREF_KEY:
			//	// TODO
			case btrfsitem.DirEntries:
				for _, dir := range body {
					fmt.Printf("\t\tlocation %v type %v\n",
						FmtKey(dir.Location), dir.Type)
					fmt.Printf("\t\ttransid %v data_len %v name_len %v\n",
						dir.TransID, dir.DataLen, dir.NameLen)
					fmt.Printf("\t\tname: %s\n", dir.Name)
					if len(dir.Data) > 0 {
						fmt.Printf("\t\tdata %v\n", dir.Data)
					}
				}
			//case btrfsitem.DIR_LOG_INDEX_KEY, btrfsitem.DIR_LOG_ITEM_KEY:
			//	// TODO
			case btrfsitem.Root:
				fmt.Printf("\t\tgeneration %v root_dirid %v bytenr %d byte_limit %v bytes_used %v\n",
					body.Generation, body.RootDirID, body.ByteNr, body.ByteLimit, body.BytesUsed)
				fmt.Printf("\t\tlast_snapshot %v flags %v refs %v\n",
					body.LastSnapshot, body.Flags, body.Refs)
				fmt.Printf("\t\tdrop_progress %v drop_level %v\n",
					FmtKey(body.DropProgress), body.DropLevel)
				fmt.Printf("\t\tlevel %v generation_v2 %v\n",
					body.Level, body.GenerationV2)
				if body.Generation == body.GenerationV2 {
					fmt.Printf("\t\tuuid %v\n", body.UUID)
					fmt.Printf("\t\tparent_uuid %v\n", body.ParentUUID)
					fmt.Printf("\t\treceived_uuid %v\n", body.ReceivedUUID)
					fmt.Printf("\t\tctransid %v otransid %v stransid %v rtransid %v\n",
						body.CTransID, body.OTransID, body.STransID, body.RTransID)
					fmt.Printf("\t\tctime %v\n", fmtTime(body.CTime))
					fmt.Printf("\t\totime %v\n", fmtTime(body.OTime))
					fmt.Printf("\t\tstime %v\n", fmtTime(body.STime))
					fmt.Printf("\t\trtime %v\n", fmtTime(body.RTime))
				}
			case btrfsitem.RootRef:
				var tag string
				switch item.Head.Key.ItemType {
				case btrfsitem.ROOT_REF_KEY:
					tag = "ref"
				case btrfsitem.ROOT_BACKREF_KEY:
					tag = "backref"
				default:
					tag = fmt.Sprintf("(error: unhandled RootRef item type: %v)", item.Head.Key.ItemType)
				}
				fmt.Printf("\t\troot %v key dirid %v sequence %v name %s\n",
					tag, body.DirID, body.Sequence, body.Name)
			case btrfsitem.Extent:
				fmt.Printf("\t\trefs %v gen %v flags %v\n",
					body.Head.Refs, body.Head.Generation, body.Head.Flags)
				if body.Head.Flags.Has(btrfsitem.EXTENT_FLAG_TREE_BLOCK) {
					fmt.Printf("\t\ttree block %v level %v\n",
						FmtKey(body.Info.Key), body.Info.Level)
				}
				printExtentInlineRefs(body.Refs)
			case btrfsitem.Metadata:
				fmt.Printf("\t\trefs %v gen %v flags %v\n",
					body.Head.Refs, body.Head.Generation, body.Head.Flags)
				fmt.Printf("\t\ttree block skinny level %v\n", item.Head.Key.Offset)
				printExtentInlineRefs(body.Refs)
			//case btrfsitem.EXTENT_DATA_REF_KEY:
			//	// TODO
			//case btrfsitem.SHARED_DATA_REF_KEY:
			//	// TODO
			case btrfsitem.ExtentCSum:
				sb, _ := fs.Superblock()
				sectorSize := btrfsvol.AddrDelta(sb.Data.SectorSize)

				start := btrfsvol.LogicalAddr(item.Head.Key.Offset)
				itemSize := btrfsvol.AddrDelta(len(body.Sums)) * sectorSize
				fmt.Printf("\t\trange start %d end %d length %d",
					start, start.Add(itemSize), itemSize)
				sumsPerLine := util.Max(1, len(btrfssum.CSum{})/body.ChecksumSize/2)

				pos := start
				for i, sum := range body.Sums {
					if i%sumsPerLine == 0 {
						fmt.Printf("\n\t\t")
					} else {
						fmt.Printf(" ")
					}
					fmt.Printf("[%d] 0x%s", pos, sum.Fmt(sb.Data.ChecksumType))
					pos = pos.Add(sectorSize)
				}
				fmt.Printf("\n")
			case btrfsitem.FileExtent:
				fmt.Printf("\t\tgeneration %v type %v\n",
					body.Generation, body.Type)
				switch body.Type {
				case btrfsitem.FILE_EXTENT_INLINE:
					fmt.Printf("\t\tinline extent data size %v ram_bytes %v compression %v\n",
						len(body.BodyInline), body.RAMBytes, body.Compression)
				case btrfsitem.FILE_EXTENT_PREALLOC:
					fmt.Printf("\t\tprealloc data disk byte %v nr %v\n",
						body.BodyPrealloc.DiskByteNr,
						body.BodyPrealloc.DiskNumBytes)
					fmt.Printf("\t\tprealloc data offset %v nr %v\n",
						body.BodyPrealloc.Offset,
						body.BodyPrealloc.NumBytes)
				case btrfsitem.FILE_EXTENT_REG:
					fmt.Printf("\t\textent data disk byte %v nr %v\n",
						body.BodyReg.DiskByteNr,
						body.BodyReg.DiskNumBytes)
					fmt.Printf("\t\textent data offset %v nr %v ram %v\n",
						body.BodyReg.Offset,
						body.BodyReg.NumBytes,
						body.RAMBytes)
					fmt.Printf("\t\textent compression %v\n",
						body.Compression)
				default:
					fmt.Printf("\t\t(error) unknown file extent type %v", body.Type)
				}
			case btrfsitem.BlockGroup:
				fmt.Printf("\t\tblock group used %v chunk_objectid %v flags %v\n",
					body.Used, body.ChunkObjectID, body.Flags)
			case btrfsitem.FreeSpaceInfo:
				fmt.Printf("\t\tfree space info extent count %v flags %v\n",
					body.ExtentCount, body.Flags)
			case btrfsitem.FreeSpaceBitmap:
				fmt.Printf("\t\tfree space bitmap\n")
			case btrfsitem.Chunk:
				fmt.Printf("\t\tlength %d owner %d stripe_len %v type %v\n",
					body.Head.Size, body.Head.Owner, body.Head.StripeLen, body.Head.Type)
				fmt.Printf("\t\tio_align %v io_width %v sector_size %v\n",
					body.Head.IOOptimalAlign, body.Head.IOOptimalWidth, body.Head.IOMinSize)
				fmt.Printf("\t\tnum_stripes %v sub_stripes %v\n",
					body.Head.NumStripes, body.Head.SubStripes)
				for i, stripe := range body.Stripes {
					fmt.Printf("\t\t\tstripe %v devid %d offset %d\n",
						i, stripe.DeviceID, stripe.Offset)
					fmt.Printf("\t\t\tdev_uuid %v\n",
						stripe.DeviceUUID)
				}
			case btrfsitem.Dev:
				fmt.Printf(""+
					"\t\tdevid %d total_bytes %v bytes_used %v\n"+
					"\t\tio_align %v io_width %v sector_size %v type %v\n"+
					"\t\tgeneration %v start_offset %v dev_group %v\n"+
					"\t\tseek_speed %v bandwidth %v\n"+
					"\t\tuuid %v\n"+
					"\t\tfsid %v\n",
					body.DevID, body.NumBytes, body.NumBytesUsed,
					body.IOOptimalAlign, body.IOOptimalWidth, body.IOMinSize, body.Type,
					body.Generation, body.StartOffset, body.DevGroup,
					body.SeekSpeed, body.Bandwidth,
					body.DevUUID,
					body.FSUUID)
			case btrfsitem.DevExtent:
				fmt.Printf(""+
					"\t\tdev extent chunk_tree %v\n"+
					"\t\tchunk_objectid %v chunk_offset %d length %d\n"+
					"\t\tchunk_tree_uuid %v\n",
					body.ChunkTree, body.ChunkObjectID, body.ChunkOffset, body.Length,
					body.ChunkTreeUUID)
			//case btrfsitem.QGROUP_STATUS_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_INFO_KEY:
			//	// TODO
			//case btrfsitem.QGROUP_LIMIT_KEY:
			//	// TODO
			case btrfsitem.UUIDMap:
				for _, subvolID := range body {
					fmt.Printf("\t\tsubvol_id %d\n",
						subvolID)
				}
			//case btrfsitem.STRING_ITEM_KEY:
			//	// TODO
			case btrfsitem.DevStats:
				fmt.Printf("\t\tpersistent item objectid %v offset %v\n",
					item.Head.Key.ObjectID.Format(item.Head.Key.ItemType), item.Head.Key.Offset)
				switch item.Head.Key.ObjectID {
				case btrfs.DEV_STATS_OBJECTID:
					fmt.Printf("\t\tdevice stats\n")
					fmt.Printf("\t\twrite_errs %v read_errs %v flush_errs %v corruption_errs %v generation %v\n",
						body.Values[btrfsitem.DEV_STAT_WRITE_ERRS],
						body.Values[btrfsitem.DEV_STAT_READ_ERRS],
						body.Values[btrfsitem.DEV_STAT_FLUSH_ERRS],
						body.Values[btrfsitem.DEV_STAT_CORRUPTION_ERRS],
						body.Values[btrfsitem.DEV_STAT_GENERATION_ERRS])
				default:
					fmt.Printf("\t\tunknown persistent item objectid %v\n", item.Head.Key.ObjectID)
				}
			//case btrfsitem.TEMPORARY_ITEM_KEY:
			//	// TODO
			case btrfsitem.Empty:
				switch item.Head.Key.ItemType {
				case btrfsitem.ORPHAN_ITEM_KEY: // 48
					fmt.Printf("\t\torphan item\n")
				case btrfsitem.TREE_BLOCK_REF_KEY: // 176
					fmt.Printf("\t\ttree block backref\n")
				case btrfsitem.SHARED_BLOCK_REF_KEY: // 182
					fmt.Printf("\t\tshared block backref\n")
				case btrfsitem.FREE_SPACE_EXTENT_KEY: // 199
					fmt.Printf("\t\tfree space extent\n")
				case btrfsitem.QGROUP_RELATION_KEY: // 246
					// do nothing
				//case btrfsitem.EXTENT_REF_V0_KEY:
				//	fmt.Printf("\t\textent ref v0 (deprecated)\n")
				//case btrfsitem.CSUM_ITEM_KEY:
				//	fmt.Printf("\t\tcsum item\n")
				default:
					fmt.Printf("\t\t(error) unhandled empty item type: %v\n", item.Head.Key.ItemType)
				}
			case btrfsitem.Error:
				fmt.Printf("\t\t(error) error item: %v\n", body.Err)
			default:
				fmt.Printf("\t\t(error) unhandled item type: %T\n", body)
			}
			return nil
		},
	})
}

// printHeaderInfo mimics btrfs-progs kernel-shared/print-tree.c:print_header_info()
func printHeaderInfo(node btrfs.Node) {
	var typename string
	if node.Head.Level > 0 { // internal node
		typename = "node"
		fmt.Printf("node %v level %v items %v free space %v",
			node.Head.Addr,
			node.Head.Level,
			node.Head.NumItems,
			node.MaxItems()-node.Head.NumItems)
	} else { // leaf node
		typename = "leaf"
		fmt.Printf("leaf %d items %v free space %v",
			node.Head.Addr,
			node.Head.NumItems,
			node.LeafFreeSpace())
	}
	fmt.Printf(" generation %v owner %v\n",
		node.Head.Generation,
		node.Head.Owner)

	fmt.Printf("%v %d flags %v backref revision %v\n",
		typename,
		node.Head.Addr,
		node.Head.Flags,
		node.Head.BackrefRev)

	fmt.Printf("checksum stored %v\n", node.Head.Checksum.Fmt(node.ChecksumType))
	if calcSum, err := node.CalculateChecksum(); err != nil {
		fmt.Printf("checksum calced %v\n", err)
	} else {
		fmt.Printf("checksum calced %v\n", calcSum.Fmt(node.ChecksumType))
	}

	fmt.Printf("fs uuid %v\n", node.Head.MetadataUUID)
	fmt.Printf("chunk uuid %v\n", node.Head.ChunkTreeUUID)
}

// printExtentInlineRefs mimics part of btrfs-progs kernel-shared/print-tree.c:print_extent_item()
func printExtentInlineRefs(refs []btrfsitem.ExtentInlineRef) {
	for _, ref := range refs {
		switch subitem := ref.Body.(type) {
		case nil:
			switch ref.Type {
			case btrfsitem.TREE_BLOCK_REF_KEY:
				fmt.Printf("\t\ttree block backref root %v\n",
					btrfs.ObjID(ref.Offset))
			case btrfsitem.SHARED_BLOCK_REF_KEY:
				fmt.Printf("\t\tshared block backref parent %v\n",
					ref.Offset)
			default:
				fmt.Printf("\t\t(error) unexpected empty sub-item type: %v\n", ref.Type)
			}
		case btrfsitem.ExtentDataRef:
			fmt.Printf("\t\textent data backref root %v objectid %v offset %v count %v\n",
				subitem.Root, subitem.ObjectID, subitem.Offset, subitem.Count)
		case btrfsitem.SharedDataRef:
			fmt.Printf("\t\tshared data backref parent %v count %v\n",
				ref.Offset, subitem.Count)
		default:
			fmt.Printf("\t\t(error) unexpected sub-item type: %T\n", subitem)
		}
	}
}

// mimics print-tree.c:btrfs_print_key()
func FmtKey(key btrfs.Key) string {
	var out strings.Builder
	fmt.Fprintf(&out, "key (%v %v", key.ObjectID.Format(key.ItemType), key.ItemType)
	switch key.ItemType {
	case btrfsitem.QGROUP_RELATION_KEY: //TODO, btrfsitem.QGROUP_INFO_KEY, btrfsitem.QGROUP_LIMIT_KEY:
		panic("not implemented")
	case btrfsitem.UUID_SUBVOL_KEY, btrfsitem.UUID_RECEIVED_SUBVOL_KEY:
		fmt.Fprintf(&out, " %#08x)", key.Offset)
	case btrfsitem.ROOT_ITEM_KEY:
		fmt.Fprintf(&out, " %v)", btrfs.ObjID(key.Offset))
	default:
		if key.Offset == util.MaxUint64pp-1 {
			fmt.Fprintf(&out, " -1)")
		} else {
			fmt.Fprintf(&out, " %v)", key.Offset)
		}
	}
	return out.String()
}

func fmtTime(t btrfs.Time) string {
	return fmt.Sprintf("%v.%v (%v)",
		t.Sec, t.NSec, t.ToStd().Format("2006-01-02 15:04:05"))
}
