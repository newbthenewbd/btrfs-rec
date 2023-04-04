// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package btrfsutil

import (
	"fmt"
	"strings"

	"github.com/datawire/dlib/derror"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsprim"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
)

func (g Graph) renderNode(node btrfsvol.LogicalAddr) []string {
	if node == 0 {
		return []string{"root"}
	} else if nodeData, ok := g.Nodes[node]; ok {
		return []string{
			fmt.Sprintf("{addr:      %v,", node),
			fmt.Sprintf(" level:     %v,", nodeData.Level),
			fmt.Sprintf(" gen:       %v,", nodeData.Generation),
			fmt.Sprintf(" num_items: %v,", len(nodeData.Items)),
			fmt.Sprintf(" min_item:  {%d,%v,%d},",
				nodeData.MinItem(g).ObjectID,
				nodeData.MinItem(g).ItemType,
				nodeData.MinItem(g).Offset),
			fmt.Sprintf(" max_item:  {%d,%v,%d}}",
				nodeData.MaxItem(g).ObjectID,
				nodeData.MaxItem(g).ItemType,
				nodeData.MaxItem(g).Offset),
		}
	} else if nodeErr, ok := g.BadNodes[node]; ok {
		return []string{
			fmt.Sprintf("{addr:%v,", node),
			fmt.Sprintf(" err:%q}", nodeErr.Error()),
		}
	} else {
		panic("should not happen")
	}
}

func (g Graph) renderEdge(kp GraphEdge) []string {
	a := fmt.Sprintf("[%d]={", kp.FromSlot)
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
	if toNode, ok := g.Nodes[kp.ToNode]; !ok {
		err = g.BadNodes[kp.ToNode]
	} else {
		err = g.loopCheckNodeExpectations(kp, toNode)
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

func (g Graph) renderLoop(stack []btrfsvol.LogicalAddr) []string {
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
			for _, kp := range g.EdgesTo[node] {
				if kp.FromNode == stack[i-1] {
					add(g.renderEdge(*kp))
					break
				}
			}
		}
		add(g.renderNode(node))
	}

	return lines
}

func (g Graph) loopCheckNodeExpectations(kp GraphEdge, toNode GraphNode) error {
	var errs derror.MultiError
	if toNode.Level != kp.ToLevel {
		errs = append(errs, fmt.Errorf("kp.level=%v != node.level=%v",
			kp.ToLevel, toNode.Level))
	}
	if toNode.Generation != kp.ToGeneration {
		errs = append(errs, fmt.Errorf("kp.generation=%v != node.generation=%v",
			kp.ToGeneration, toNode.Generation))
	}
	switch {
	case len(toNode.Items) == 0:
		errs = append(errs, fmt.Errorf("node.num_items=0"))
	case kp.ToKey != (btrfsprim.Key{}) && toNode.MinItem(g) != kp.ToKey:
		errs = append(errs, fmt.Errorf("kp.key=%v != node.items[0].key=%v",
			kp.ToKey, toNode.MinItem(g)))
	}
	if len(errs) > 0 {
		return errs
	}
	return nil
}
