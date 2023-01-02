// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"context"
	"os"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsprogs/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

type subcommand struct {
	cobra.Command
	RunE func(*btrfs.FS, *cobra.Command, []string) error
}

var inspectors, repairers []subcommand

func main() {
	logLevelFlag := textui.LogLevelFlag{
		Level: dlog.LogLevelInfo,
	}
	var pvsFlag []string
	var mappingsFlag string

	argparser := &cobra.Command{
		Use:   "btrfs-rec {[flags]|SUBCOMMAND}",
		Short: "Recover (data from) a broken btrfs filesystem",

		Args: cliutil.WrapPositionalArgs(cliutil.OnlySubcommands),
		RunE: cliutil.RunSubcommands,

		SilenceErrors: true, // main() will handle this after .ExecuteContext() returns
		SilenceUsage:  true, // our FlagErrorFunc will handle it

		CompletionOptions: cobra.CompletionOptions{
			DisableDefaultCmd: true,
		},
	}
	argparser.SetFlagErrorFunc(cliutil.FlagErrorFunc)
	argparser.SetHelpTemplate(cliutil.HelpTemplate)
	argparser.PersistentFlags().Var(&logLevelFlag, "verbosity", "set the verbosity")
	argparser.PersistentFlags().StringArrayVar(&pvsFlag, "pv", nil, "open the file `physical_volume` as part of the filesystem")
	if err := argparser.MarkPersistentFlagFilename("pv"); err != nil {
		panic(err)
	}
	if err := argparser.MarkPersistentFlagRequired("pv"); err != nil {
		panic(err)
	}
	argparser.PersistentFlags().StringVar(&mappingsFlag, "mappings", "", "load chunk/dev-extent/blockgroup data from external JSON file `mappings.json`")
	if err := argparser.MarkPersistentFlagFilename("mappings"); err != nil {
		panic(err)
	}

	openFlag := os.O_RDONLY

	argparserInspect := &cobra.Command{
		Use:   "inspect {[flags]|SUBCOMMAND}",
		Short: "Inspect (but don't modify) a broken btrfs filesystem",

		Args: cliutil.WrapPositionalArgs(cliutil.OnlySubcommands),
		RunE: cliutil.RunSubcommands,
	}
	argparser.AddCommand(argparserInspect)

	argparserRepair := &cobra.Command{
		Use:   "repair {[flags]|SUBCOMMAND}",
		Short: "Repair a broken btrfs filesystem",

		Args: cliutil.WrapPositionalArgs(cliutil.OnlySubcommands),
		RunE: cliutil.RunSubcommands,

		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			openFlag = os.O_RDWR
			return nil
		},
	}
	argparser.AddCommand(argparserRepair)

	for _, cmdgrp := range []struct {
		parent   *cobra.Command
		children []subcommand
	}{
		{argparserInspect, inspectors},
		{argparserRepair, repairers},
	} {
		for _, child := range cmdgrp.children {
			cmd := child.Command
			runE := child.RunE
			cmd.RunE = func(cmd *cobra.Command, args []string) error {
				ctx := cmd.Context()
				logger := textui.NewLogger(os.Stderr, logLevelFlag.Level)
				ctx = dlog.WithLogger(ctx, logger)
				ctx = dlog.WithField(ctx, "mem", new(textui.LiveMemUse))
				dlog.SetFallbackLogger(logger.WithField("btrfs-progs.THIS_IS_A_BUG", true))

				grp := dgroup.NewGroup(ctx, dgroup.GroupConfig{
					EnableSignalHandling: true,
				})
				grp.Go("main", func(ctx context.Context) (err error) {
					maybeSetErr := func(_err error) {
						if _err != nil && err == nil {
							err = _err
						}
					}
					fs, err := btrfsutil.Open(ctx, openFlag, pvsFlag...)
					if err != nil {
						return err
					}
					defer func() {
						maybeSetErr(fs.Close())
					}()

					if mappingsFlag != "" {
						mappingsJSON, err := readJSONFile[[]btrfsvol.Mapping](ctx, mappingsFlag)
						if err != nil {
							return err
						}
						for _, mapping := range mappingsJSON {
							if err := fs.LV.AddMapping(mapping); err != nil {
								return err
							}
						}
					}

					cmd.SetContext(ctx)
					return runE(fs, cmd, args)
				})
				return grp.Wait()
			}
			cmdgrp.parent.AddCommand(&cmd)
		}
	}

	if err := argparser.ExecuteContext(context.Background()); err != nil {
		textui.Fprintf(os.Stderr, "%v: error: %v\n", argparser.CommandPath(), err)
		os.Exit(1)
	}
}
