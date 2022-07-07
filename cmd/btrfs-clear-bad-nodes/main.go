package main

import (
	"fmt"
	"os"

	"lukeshu.com/btrfs-tools/pkg/btrfs"
	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/btrfsmisc"
	"lukeshu.com/btrfs-tools/pkg/util"
)

func main() {
	if err := Main(os.Args[1:]...); err != nil {
		fmt.Fprintf(os.Stderr, "%v: error: %v\n", os.Args[0], err)
		os.Exit(1)
	}
}

func Main(imgfilenames ...string) (err error) {
	maybeSetErr := func(_err error) {
		if _err != nil && err == nil {
			err = _err
		}
	}

	fs, err := btrfsmisc.Open(os.O_RDWR, imgfilenames...)
	if err != nil {
		return err
	}
	defer func() {
		maybeSetErr(fs.Close())
	}()

	var treeName string
	btrfsmisc.WalkFS(fs, btrfsmisc.WalkFSHandler{
		PreTree: func(name string, _ btrfsvol.LogicalAddr) {
			treeName = name
		},
		Err: func(err error) {
			fmt.Printf("error: %v\n", err)
		},
		UnsafeNodes: true,
		TreeWalkHandler: btrfs.TreeWalkHandler{
			Node: func(path btrfs.TreePath, node *util.Ref[btrfsvol.LogicalAddr, btrfs.Node], err error) error {
				if node == nil || err == nil {
					return nil
				}
				origErr := err
				if len(path) < 2 {
					// TODO(lukeshu): Get info from the superblock and such
					// instead of the parent node, so that we can repair broken
					// root nodes.
					return fmt.Errorf("root node: %w", err)
				}
				parentNode, err := fs.ReadNode(path[len(path)-2].NodeAddr)
				if err != nil {
					return err
				}
				node.Data = btrfs.Node{
					Size:         node.Data.Size,
					ChecksumType: node.Data.ChecksumType,
					Head: btrfs.NodeHeader{
						//Checksum:   filled below,
						MetadataUUID: parentNode.Data.Head.MetadataUUID,
						Addr:         node.Addr,
						Flags:        btrfs.NodeWritten,
						BackrefRev:   parentNode.Data.Head.BackrefRev,
						Generation:   0,
						Owner:        parentNode.Data.Head.Owner,
						NumItems:     0,
						Level:        parentNode.Data.Head.Level - 1,
					},
				}
				node.Data.Head.Checksum, err = node.Data.CalculateChecksum()
				if err != nil {
					return btrfsmisc.WalkErr{
						TreeName: treeName,
						Path:     path,
						Err:      err,
					}
				}
				if err := node.Write(); err != nil {
					return err
				}

				fmt.Printf("fixed node@%v (err was %v)\n", node.Addr, origErr)
				return nil
			},
		},
	})

	return nil
}
