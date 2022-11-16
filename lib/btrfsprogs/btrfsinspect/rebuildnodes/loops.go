// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package rebuildnodes

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/datawire/dlib/derror"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/slices"
)

func ShowLoops(ctx context.Context, out io.Writer, fs *btrfs.FS, nodeScanResults btrfsinspect.ScanDevicesResult) error {
	scanData, err := ScanDevices(ctx, fs, nodeScanResults)
	if err != nil {
		return err
	}

	dlog.Info(ctx, "Collecting orphans...")
	orphans := make(containers.Set[btrfsvol.LogicalAddr])
	for node := range scanData.Nodes {
		if len(scanData.EdgesTo[node]) == 0 {
			orphans.Insert(node)
		}
	}

	dlog.Info(ctx, "Walking graph...")
	loopWalk(out, *scanData, 0)
	for _, orphan := range maps.SortedKeys(orphans) {
		loopWalk(out, *scanData, orphan)
	}

	return nil
}

func loopWalk(out io.Writer, scanData scanResult, stack ...btrfsvol.LogicalAddr) {
	for _, kp := range scanData.EdgesFrom[stack[len(stack)-1]] {
		childStack := append(stack, kp.ToNode)
		if slices.Contains(kp.ToNode, stack) {
			loopRender(out, scanData, childStack...)
		} else {
			loopWalk(out, scanData, childStack...)
		}
	}
}

func nodeRender(scanData scanResult, node btrfsvol.LogicalAddr) []string {
	if node == 0 {
		return []string{"root"}
	} else if nodeData, ok := scanData.Nodes[node]; ok {
		return []string{
			fmt.Sprintf("{addr:      %v,", node),
			fmt.Sprintf(" level:     %v,", nodeData.Level),
			fmt.Sprintf(" gen:       %v,", nodeData.Generation),
			fmt.Sprintf(" num_items: %v,", nodeData.NumItems),
			fmt.Sprintf(" min_item:  {%d,%v,%d},",
				nodeData.MinItem.ObjectID,
				nodeData.MinItem.ItemType,
				nodeData.MinItem.Offset),
			fmt.Sprintf(" max_item:  {%d,%v,%d}}",
				nodeData.MaxItem.ObjectID,
				nodeData.MaxItem.ItemType,
				nodeData.MaxItem.Offset),
		}
	} else if nodeErr, ok := scanData.BadNodes[node]; ok {
		return []string{
			fmt.Sprintf("{addr:%v,", node),
			fmt.Sprintf(" err:%q}", nodeErr.Error()),
		}
	} else {
		panic("should not happen")
	}
}

func edgeRender(scanData scanResult, kp kpData) []string {
	a := fmt.Sprintf("[%d]={", kp.FromItem)
	b := strings.Repeat(" ", len(a))
	ret := []string{
		a + fmt.Sprintf("ToNode:  %v,", kp.ToNode),
		b + fmt.Sprintf("ToLevel: %v,", kp.ToLevel),
		b + fmt.Sprintf("ToGen:   %v,", kp.ToGeneration),
		b + fmt.Sprintf("ToKey:   {%d,%v,%d}}",
			kp.ToKey.ObjectID,
			kp.ToKey.ItemType,
			kp.ToKey.Offset),
	}

	var err error
	if toNode, ok := scanData.Nodes[kp.ToNode]; !ok {
		err = scanData.BadNodes[kp.ToNode]
	} else {
		err = checkNodeExpectations(kp, toNode)
	}
	if err != nil {
		c := strings.Repeat(" ", len(a)-1)
		ret = append(ret,
			c+"^",
			c+"`-err="+strings.ReplaceAll(err.Error(), "\n", "\n"+c+"      "),
		)
	}
	return ret
}

func loopRender(out io.Writer, scanData scanResult, stack ...btrfsvol.LogicalAddr) {
	var lines []string
	add := func(suffixes []string) {
		curLen := 0
		for _, line := range lines {
			if len(line) > curLen {
				curLen = len(line)
			}
		}
		for i, suffix := range suffixes {
			if len(lines) <= i {
				lines = append(lines, "")
			}
			if len(lines[i]) < curLen {
				if i == 0 {
					lines[i] += strings.Repeat("-", curLen-len(lines[i])-1) + ">"
				} else {
					lines[i] += strings.Repeat(" ", curLen-len(lines[i]))
				}
			}
			lines[i] += suffix
		}
	}

	for i, node := range stack {
		if i > 0 {
			for _, kp := range scanData.EdgesTo[node] {
				if kp.FromNode == stack[i-1] {
					add(edgeRender(scanData, *kp))
					break
				}
			}
		}
		add(nodeRender(scanData, node))
	}

	fmt.Fprintln(out, "loop:")
	for _, line := range lines {
		fmt.Fprintln(out, "    "+line)
	}
}

func checkNodeExpectations(kp kpData, toNode nodeData) error {
	var errs derror.MultiError
	if toNode.Level != kp.ToLevel {
		errs = append(errs, fmt.Errorf("kp.level=%v != node.level=%v",
			kp.ToLevel, toNode.Level))
	}
	if toNode.Generation != kp.ToGeneration {
		errs = append(errs, fmt.Errorf("kp.generation=%v != node.generation=%v",
			kp.ToGeneration, toNode.Generation))
	}
	if toNode.NumItems == 0 {
		errs = append(errs, fmt.Errorf("node.num_items=0"))
	} else if kp.ToKey != (btrfsprim.Key{}) && toNode.MinItem != kp.ToKey {
		errs = append(errs, fmt.Errorf("kp.key=%v != node.items[0].key=%v",
			kp.ToKey, toNode.MinItem))
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
