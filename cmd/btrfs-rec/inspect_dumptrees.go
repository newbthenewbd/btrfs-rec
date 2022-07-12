// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"fmt"
	"os"

	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func init() {
	inspectors = append(inspectors, subcommand{
		Command: cobra.Command{
			Use:   "dump-trees",
			Short: "A clone of `btrfs inspect-internal dump-tree`",
			Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, _ *cobra.Command, _ []string) error {
			const version = "5.18.1"
			fmt.Printf("btrfs-progs v%v\n", version)
			return btrfsinspect.DumpTrees(os.Stdout, os.Stderr, fs)
		},
	})
}
