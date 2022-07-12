// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsrepair"
)

func init() {
	repairers = append(repairers, subcommand{
		Command: cobra.Command{
			Use:   "clear-bad-nodes",
			Short: "Overwrite corrupt nodes with empty nodes",
			Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, _ []string) error {
			return btrfsrepair.ClearBadNodes(cmd.Context(), fs)
		},
	})
}
