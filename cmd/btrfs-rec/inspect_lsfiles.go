// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"os"

	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/lsfiles"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
)

func init() {
	inspectors.AddCommand(&cobra.Command{
		Use:   "ls-files",
		Short: "A listing of all files in the filesystem",
		Args:  cliutil.WrapPositionalArgs(cobra.NoArgs),
		RunE: runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, _ []string) (err error) {
			out := bufio.NewWriter(os.Stdout)
			defer func() {
				if _err := out.Flush(); _err != nil && err == nil {
					err = _err
				}
			}()

			return lsfiles.LsFiles(
				cmd.Context(),
				out,
				btrfsutil.NewOldRebuiltForrest(fs))
		}),
	})
}
