// Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsinspect"
)

func init() {
	var skipFileSums bool
	cmd := subcommand{
		Command: cobra.Command{
			Use:   "mount MOUNTPOINT",
			Short: "Mount the filesystem read-only",
			Args:  cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		},
		RunE: func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
			return btrfsinspect.MountRO(cmd.Context(), fs, args[0], skipFileSums)
		},
	}
	cmd.Command.Flags().BoolVar(&skipFileSums, "skip-filesums", false,
		"ignore checksum failures on file contents; allow such files to be read")
	inspectors = append(inspectors, cmd)
}
