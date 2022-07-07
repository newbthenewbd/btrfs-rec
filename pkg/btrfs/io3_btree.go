package btrfs

import (
	"errors"
	"fmt"
	"io"
	iofs "io/fs"
	"math"
	"strings"

	"github.com/datawire/dlib/derror"

	"lukeshu.com/btrfs-tools/pkg/btrfs/btrfsvol"
	"lukeshu.com/btrfs-tools/pkg/util"
)

// - The first element will always have an ItemIdx of -1.
//
// - For .Item() callbacks, the last element will always have a
//   NodeAddr of 0.
//
// For example, given the tree structure
//
//   [superblock]
//        |
//        | <------------------------------------------ pathElem={idx:-1, addr:0x01, lvl:3}
//        |
//     +[0x01]-----------+
//     | lvl=3           |
//     +-+-+-+-+-+-+-+-+-+
//     |1|2|3|4|5|6|7|8|9|
//     +---+---+---+---+-+
//                    |
//                    | <------------------------------ pathElem={idx:8, addr:0x02, lvl:2}
//                    |
//                 +[0x02]-----------+
//                 | lvl=2           |
//                 +-+-+-+-+-+-+-+-+-+
//                 |1|2|3|4|5|6|7|8|9|
//                 +---+---+---+---+-+
//                              |
//                              | <-------------------- pathElem={idx:7, addr:0x03, lvl:1}
//                              |
//                           +[0x03]-----------+
//                           | lvl=1           |
//                           +-+-+-+-+-+-+-+-+-+
//                           |1|2|3|4|5|6|7|8|9|
//                           +---+---+---+---+-+
//                                  |
//                                  | <---------------- pathElem={idx:4, addr:0x04, lvl:0}
//                                  |
//                                +[0x04]-----------+
//                                | lvl=0           |
//                                +-+-+-+-+-+-+-+-+-+
//                                |1|2|3|4|5|6|7|8|9|
//                                +---+---+---+---+-+
//                                   |
//                                   | <--------------- pathElem={idx:5, addr:0, lvl:0}
//                                   |
//                                 [item]
//
// the path would be
//
//     {-1, 0x01, 3}→{8, 0x02, 2}→{7, 0x03, 1}→{4, 0x04, 0}→{2, 0, 0}
type TreePath []TreePathElem

// A TreePathElem essentially represents a KeyPointer.
type TreePathElem struct {
	// ItemIdx is the index of this KeyPointer in the parent Node;
	// or -1 if this is the root and there is no KeyPointer.
	ItemIdx int
	// NodeAddr is the address of the node that the KeyPointer
	// points at, or 0 if this is a leaf item and nothing is
	// being pointed at.
	NodeAddr btrfsvol.LogicalAddr
	// NodeLevel is the expected or actual level of the node at
	// NodeAddr, or 255 if there is no knowledge of the level.
	NodeLevel uint8
}

func (elem TreePathElem) writeNodeTo(w io.Writer) {
	if elem.NodeLevel != math.MaxUint8 {
		fmt.Fprintf(w, "node:%d@%v", elem.NodeLevel, elem.NodeAddr)
	} else {
		fmt.Fprintf(w, "node@%v", elem.NodeAddr)
	}
}

func (path TreePath) String() string {
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
	PreNode  func(TreePath) error
	Node     func(TreePath, *util.Ref[btrfsvol.LogicalAddr, Node], error) error
	PostNode func(TreePath, *util.Ref[btrfsvol.LogicalAddr, Node]) error
	// Callbacks for items on internal nodes
	PreKeyPointer  func(TreePath, KeyPointer) error
	PostKeyPointer func(TreePath, KeyPointer) error
	// Callbacks for items on leaf nodes
	Item func(TreePath, Item) error
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
func (fs *FS) TreeWalk(treeRoot btrfsvol.LogicalAddr, cbs TreeWalkHandler) error {
	path := TreePath{
		TreePathElem{
			ItemIdx:   -1,
			NodeAddr:  treeRoot,
			NodeLevel: math.MaxUint8,
		},
	}
	return fs.treeWalk(path, cbs)
}

func (fs *FS) treeWalk(path TreePath, cbs TreeWalkHandler) error {
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
	node, err := fs.readNodeAtLevel(path[len(path)-1].NodeAddr, path[len(path)-1].NodeLevel)
	if node != nil {
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
			itemPath := append(path, TreePathElem{
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
				itemPath := append(path, TreePathElem{
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

func (fs *FS) treeSearch(treeRoot btrfsvol.LogicalAddr, fn func(Key) int) (TreePath, *util.Ref[btrfsvol.LogicalAddr, Node], error) {
	path := TreePath{
		TreePathElem{
			ItemIdx:   -1,
			NodeAddr:  treeRoot,
			NodeLevel: math.MaxUint8,
		},
	}
	for {
		if path[len(path)-1].NodeAddr == 0 {
			return nil, nil, iofs.ErrNotExist
		}
		node, err := fs.readNodeAtLevel(path[len(path)-1].NodeAddr, path[len(path)-1].NodeLevel)
		if err != nil {
			return nil, nil, err
		}
		path[len(path)-1].NodeLevel = node.Data.Head.Level

		if node.Data.Head.Level > 0 {
			// internal node

			// Search for the right-most node.Data.BodyInternal item for which
			// `fn(item.Key) >= 0`.
			//
			//    + + + + 0 - - - -
			//
			// There may or may not be a value that returns '0'.
			//
			// Implement this search as a binary search.
			lastGood := -1
			firstBad := len(node.Data.BodyInternal)
			for firstBad > lastGood+1 {
				midpoint := (lastGood + firstBad) / 2
				direction := fn(node.Data.BodyInternal[midpoint].Key)
				if direction < 0 {
					firstBad = midpoint
				} else {
					lastGood = midpoint
				}
			}
			if lastGood < 0 {
				return nil, nil, iofs.ErrNotExist
			}
			path = append(path, TreePathElem{
				ItemIdx:   lastGood,
				NodeAddr:  node.Data.BodyInternal[lastGood].BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
			})
		} else {
			// leaf node

			// Search for a member of node.Data.BodyLeaf for which
			// `fn(item.Head.Key) == 0`.
			//
			//    + + + + 0 - - - -
			//
			// Such an item might not exist; in this case, return nil/ErrNotExist.
			// Multiple such items might exist; in this case, it does not matter which
			// is returned.
			//
			// Implement this search as a binary search.
			beg := 0
			end := len(node.Data.BodyLeaf) - 1
			for beg < end {
				midpoint := (beg + end) / 2
				direction := fn(node.Data.BodyLeaf[midpoint].Head.Key)
				switch {
				case direction < 0:
					end = midpoint
				case direction > 0:
					beg = midpoint + 1
				case direction == 0:
					path = append(path, TreePathElem{
						ItemIdx: midpoint,
					})
					return path, node, nil
				}
			}
			return nil, nil, iofs.ErrNotExist
		}
	}
}

func (fs *FS) prev(path TreePath, node *util.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *util.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = append(TreePath(nil), path...)

	// go up
	for path[len(path)-1].ItemIdx < 1 {
		path = path[:len(path)-1]
		if len(path) == 0 {
			return nil, nil, nil
		}
	}
	// go left
	path[len(path)-1].ItemIdx--
	if path[len(path)-1].NodeAddr != 0 {
		if node.Addr != path[len(path)-2].NodeAddr {
			node, err = fs.readNodeAtLevel(path[len(path)-2].NodeAddr, path[len(path)-2].NodeLevel)
			if err != nil {
				return nil, nil, err
			}
			path[len(path)-1].NodeAddr = node.Data.BodyInternal[path[len(path)-1].ItemIdx].BlockPtr
		}
	}
	// go down
	for path[len(path)-1].NodeAddr != 0 {
		if node.Addr != path[len(path)-1].NodeAddr {
			node, err = fs.readNodeAtLevel(path[len(path)-1].NodeAddr, path[len(path)-1].NodeLevel)
			if err != nil {
				return nil, nil, err
			}
		}
		if node.Data.Head.Level > 0 {
			path = append(path, TreePathElem{
				ItemIdx:   len(node.Data.BodyInternal) - 1,
				NodeAddr:  node.Data.BodyInternal[len(node.Data.BodyInternal)-1].BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
			})
		} else {
			path = append(path, TreePathElem{
				ItemIdx: len(node.Data.BodyLeaf) - 1,
			})
		}
	}
	// return
	if node.Addr != path[len(path)-2].NodeAddr {
		node, err = fs.readNodeAtLevel(path[len(path)-2].NodeAddr, path[len(path)-2].NodeLevel)
		if err != nil {
			return nil, nil, err
		}
	}
	return path, node, nil
}

func (fs *FS) next(path TreePath, node *util.Ref[btrfsvol.LogicalAddr, Node]) (TreePath, *util.Ref[btrfsvol.LogicalAddr, Node], error) {
	var err error
	path = append(TreePath(nil), path...)

	// go up
	if node.Addr != path[len(path)-2].NodeAddr {
		node, err = fs.readNodeAtLevel(path[len(path)-2].NodeAddr, path[len(path)-2].NodeLevel)
		if err != nil {
			return nil, nil, err
		}
		path[len(path)-2].NodeLevel = node.Data.Head.Level
	}
	for path[len(path)-1].ItemIdx+1 >= int(node.Data.Head.NumItems) {
		path = path[:len(path)-1]
		if len(path) == 1 {
			return nil, nil, nil
		}
		if node.Addr != path[len(path)-2].NodeAddr {
			node, err = fs.readNodeAtLevel(path[len(path)-2].NodeAddr, path[len(path)-2].NodeLevel)
			if err != nil {
				return nil, nil, err
			}
			path[len(path)-2].NodeLevel = node.Data.Head.Level
		}
	}
	// go left
	path[len(path)-1].ItemIdx++
	if path[len(path)-1].NodeAddr != 0 {
		if node.Addr != path[len(path)-2].NodeAddr {
			node, err = fs.readNodeAtLevel(path[len(path)-2].NodeAddr, path[len(path)-2].NodeLevel)
			if err != nil {
				return nil, nil, err
			}
			path[len(path)-1].NodeAddr = node.Data.BodyInternal[path[len(path)-1].ItemIdx].BlockPtr
		}
	}
	// go down
	for path[len(path)-1].NodeAddr != 0 {
		if node.Addr != path[len(path)-1].NodeAddr {
			node, err = fs.readNodeAtLevel(path[len(path)-1].NodeAddr, path[len(path)-1].NodeLevel)
			if err != nil {
				return nil, nil, err
			}
			path[len(path)-1].NodeLevel = node.Data.Head.Level
		}
		if node.Data.Head.Level > 0 {
			path = append(path, TreePathElem{
				ItemIdx:   0,
				NodeAddr:  node.Data.BodyInternal[len(node.Data.BodyInternal)-1].BlockPtr,
				NodeLevel: node.Data.Head.Level - 1,
			})
		} else {
			path = append(path, TreePathElem{
				ItemIdx: 0,
			})
		}
	}
	// return
	if node.Addr != path[len(path)-2].NodeAddr {
		node, err = fs.readNodeAtLevel(path[len(path)-2].NodeAddr, path[len(path)-2].NodeLevel)
		if err != nil {
			return nil, nil, err
		}
	}
	return path, node, nil
}

func (fs *FS) TreeSearch(treeRoot btrfsvol.LogicalAddr, fn func(Key) int) (Item, error) {
	path, node, err := fs.treeSearch(treeRoot, fn)
	if err != nil {
		return Item{}, err
	}
	return node.Data.BodyLeaf[path[len(path)-1].ItemIdx], nil
}

func (fs *FS) TreeLookup(treeRoot btrfsvol.LogicalAddr, key Key) (Item, error) {
	return fs.TreeSearch(treeRoot, key.Cmp)
}

// If some items are able to be read, but there is an error reading the full set, then it might
// return *both* a list of items and an error.
//
// If no such item is found, an error that is io/fs.ErrNotExist is returned.
func (fs *FS) TreeSearchAll(treeRoot btrfsvol.LogicalAddr, fn func(Key) int) ([]Item, error) {
	middlePath, middleNode, err := fs.treeSearch(treeRoot, fn)
	if err != nil {
		return nil, err
	}
	middleItem := middleNode.Data.BodyLeaf[middlePath[len(middlePath)-1].ItemIdx]

	var ret = []Item{middleItem}
	var errs derror.MultiError
	for prevPath, prevNode := middlePath, middleNode; true; {
		prevPath, prevNode, err = fs.prev(prevPath, prevNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if prevPath == nil {
			break
		}
		prevItem := prevNode.Data.BodyLeaf[prevPath[len(prevPath)-1].ItemIdx]
		if fn(prevItem.Head.Key) != 0 {
			break
		}
		ret = append(ret, prevItem)
	}
	util.ReverseSlice(ret)
	for nextPath, nextNode := middlePath, middleNode; true; {
		nextPath, nextNode, err = fs.next(nextPath, nextNode)
		if err != nil {
			errs = append(errs, err)
			break
		}
		if nextPath == nil {
			break
		}
		nextItem := nextNode.Data.BodyLeaf[nextPath[len(nextPath)-1].ItemIdx]
		if fn(nextItem.Head.Key) != 0 {
			break
		}
		ret = append(ret, nextItem)
	}
	if errs != nil {
		err = errs
	}
	return ret, err
}
