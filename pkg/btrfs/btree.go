package btrfs

import (
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"math"
	"strings"

	"lukeshu.com/btrfs-tools/pkg/util"
)

// A TreeWalkPathElem essentially represents a KeyPointer.
type TreeWalkPathElem struct {
	// ItemIdx is the index of this KeyPointer in the parent Node;
	// or -1 if this is the root and there is no KeyPointer.
	ItemIdx int
	// NodeAddr is the address of the node that the KeyPointer
	// points at, or 0 if this is a leaf item and nothing is
	// being pointed at.
	NodeAddr LogicalAddr
	// NodeLevel is the expected or actual level of the node at
	// NodeAddr, or 255 if there is no knowledge of the level.
	NodeLevel uint8
}

func (elem TreeWalkPathElem) writeNodeTo(w io.Writer) {
	if elem.NodeLevel != math.MaxUint8 {
		fmt.Fprintf(w, "node:%d@%v", elem.NodeLevel, elem.NodeAddr)
	} else {
		fmt.Fprintf(w, "node@%v", elem.NodeAddr)
	}
}

// - The first element will always have an ItemIdx of -1.
//
// - For .Item() callbacks, the last element will always have a
//   NodeAddr of 0.
type TreeWalkPath []TreeWalkPathElem

func (path TreeWalkPath) String() string {
	if len(path) == 0 {
		return "(empty-path)"
	}
	var ret strings.Builder
	path[0].writeNodeTo(&ret)
	for _, elem := range path[1:] {
		fmt.Fprintf(&ret, "[%v]", elem.ItemIdx)
		if elem.NodeAddr != 0 {
			ret.WriteString("->")
			elem.writeNodeTo(&ret)
		}
	}
	return ret.String()
}

type TreeWalkHandler struct {
	// Callbacks for entire nodes
	PreNode  func(TreeWalkPath) error
	Node     func(TreeWalkPath, *util.Ref[LogicalAddr, Node], error) error
	PostNode func(TreeWalkPath, *util.Ref[LogicalAddr, Node]) error
	// Callbacks for items on internal nodes
	PreKeyPointer  func(TreeWalkPath, KeyPointer) error
	PostKeyPointer func(TreeWalkPath, KeyPointer) error
	// Callbacks for items on leaf nodes
	Item func(TreeWalkPath, Item) error
}

// The lifecycle of callbacks is:
//
//     001 .PreNode()
//     002 (read node)
//     003 .Node()
//         for item in node.items:
//           if internal:
//     004     .PreKeyPointer()
//     005     (recurse)
//     006     .PostKeyPointer()
//           else:
//     004     .Item()
//     007 .PostNode()
func (fs *FS) TreeWalk(treeRoot LogicalAddr, cbs TreeWalkHandler) error {
	path := TreeWalkPath{
		TreeWalkPathElem{
			ItemIdx:   -1,
			NodeAddr:  treeRoot,
			NodeLevel: math.MaxUint8,
		},
	}
	return fs.treeWalk(path, cbs)
}

func (fs *FS) treeWalk(path TreeWalkPath, cbs TreeWalkHandler) error {
	if path[len(path)-1].NodeAddr == 0 {
		return nil
	}

	if cbs.PreNode != nil {
		if err := cbs.PreNode(path); err != nil {
			if errors.Is(err, iofs.SkipDir) {
				return nil
			}
			return err
		}
	}
	node, err := fs.ReadNode(path[len(path)-1].NodeAddr)
	if node != nil {
		if exp := path[len(path)-1].NodeLevel; exp != math.MaxUint8 && node.Data.Head.Level != exp && err == nil {
			err = fmt.Errorf("btrfs.FS.TreeWalk: node@%v: expected level %v but has level %v",
				node.Addr, exp, node.Data.Head.Level)
		}
		path[len(path)-1].NodeLevel = node.Data.Head.Level
	}
	if cbs.Node != nil {
		err = cbs.Node(path, node, err)
	}
	if err != nil {
		if errors.Is(err, iofs.SkipDir) {
			return nil
		}
		return fmt.Errorf("btrfs.FS.TreeWalk: %w", err)
	}
	if node != nil {
		for i, item := range node.Data.BodyInternal {
			itemPath := append(path, TreeWalkPathElem{
				ItemIdx:   i,
				NodeAddr:  item.BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
			})
			if cbs.PreKeyPointer != nil {
				if err := cbs.PreKeyPointer(itemPath, item); err != nil {
					if errors.Is(err, iofs.SkipDir) {
						continue
					}
					return err
				}
			}
			if err := fs.treeWalk(itemPath, cbs); err != nil {
				return err
			}
			if cbs.PostKeyPointer != nil {
				if err := cbs.PostKeyPointer(itemPath, item); err != nil {
					if errors.Is(err, iofs.SkipDir) {
						continue
					}
					return err
				}
			}
		}
		for i, item := range node.Data.BodyLeaf {
			if cbs.Item != nil {
				itemPath := append(path, TreeWalkPathElem{
					ItemIdx: i,
				})
				if err := cbs.Item(itemPath, item); err != nil {
					if errors.Is(err, iofs.SkipDir) {
						continue
					}
					return fmt.Errorf("btrfs.FS.TreeWalk: callback: %w", err)
				}
			}
		}
	}
	if cbs.PostNode != nil {
		if err := cbs.PostNode(path, node); err != nil {
			if errors.Is(err, iofs.SkipDir) {
				return nil
			}
			return err
		}
	}
	return nil
}
