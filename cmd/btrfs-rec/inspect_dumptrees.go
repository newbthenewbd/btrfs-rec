// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"os"

	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/dumptrees"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

func init() {
	inspectors.AddCommand(&cobra.Command{
		Use:   "dump-trees",
		Short: "A clone of `btrfs inspect-internal dump-tree`",
		Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		RunE: runWithReadableFS(func(fs btrfs.ReadableFS, cmd *cobra.Command, _ []string) error {
			const version = "6.1.3"
			out := os.Stdout
			textui.Fprintf(out, "btrfs-progs v%v\n", version)
			dumptrees.DumpTrees(cmd.Context(), out, fs)
			return nil
		}),
	})
}
