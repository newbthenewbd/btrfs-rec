// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/mount"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
)

func init() {
	var skipFileSums bool
	cmd := &cobra.Command{
		Use:   "mount MOUNTPOINT",
		Short: "Mount the filesystem read-only",
		Args:  cliutil.WrapPositionalArgs(cobra.ExactArgs(1)),
		RunE: runWithReadableFS(func(fs btrfs.ReadableFS, cmd *cobra.Command, args []string) error {
			return mount.MountRO(cmd.Context(), fs, args[0], skipFileSums)
		}),
	}
	cmd.Flags().BoolVar(&skipFileSums, "skip-filesums", false,
		"ignore checksum failures on file contents; allow such files to be read")

	inspectors.AddCommand(cmd)
}
