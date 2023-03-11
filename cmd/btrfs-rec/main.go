// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

// Command btrfs-rec is used to recover (data from) a broken btrfs
// filesystem.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/datawire/dlib/dgroup"
	"github.com/datawire/dlib/dlog"
	"github.com/datawire/ocibuild/pkg/cliutil"
	"github.com/spf13/cobra"

	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfsutil"
	"git.lukeshu.com/btrfs-progs-ng/lib/profile"
	"git.lukeshu.com/btrfs-progs-ng/lib/textui"
)

var (
	inspectors = &cobra.Command{
		Use:   "inspect {[flags]|SUBCOMMAND}",
		Short: "Inspect (but don't modify) a broken btrfs filesystem",

		Args: cliutil.WrapPositionalArgs(cliutil.OnlySubcommands),
		RunE: cliutil.RunSubcommands,
	}
	repairers = &cobra.Command{
		Use:   "repair {[flags]|SUBCOMMAND}",
		Short: "Repair a broken btrfs filesystem",

		Args: cliutil.WrapPositionalArgs(cliutil.OnlySubcommands),
		RunE: cliutil.RunSubcommands,

		PersistentPreRunE: func(_ *cobra.Command, _ []string) error {
			globalFlags.openFlag = os.O_RDWR
			return nil
		},
	}
)

var globalFlags struct {
	logLevel textui.LogLevelFlag
	pvs      []string
	mappings string

	stopProfiling profile.StopFunc

	openFlag int
}

func noError(err error) {
	if err != nil {
		panic(fmt.Errorf("should not happen: %w", err))
	}
}

func main() {
	// Base argparser

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

	// Global flags

	globalFlags.logLevel.Level = dlog.LogLevelInfo
	argparser.PersistentFlags().Var(&globalFlags.logLevel, "verbosity", "set the verbosity")

	argparser.PersistentFlags().StringArrayVar(&globalFlags.pvs, "pv", nil,
		"open the file `physical_volume` as part of the filesystem")
	noError(argparser.MarkPersistentFlagFilename("pv"))

	argparser.PersistentFlags().StringVar(&globalFlags.mappings, "mappings", "",
		"load chunk/dev-extent/blockgroup data from external JSON file `mappings.json`")
	noError(argparser.MarkPersistentFlagFilename("mappings"))

	globalFlags.stopProfiling = profile.AddProfileFlags(argparser.PersistentFlags(), "profile.")

	globalFlags.openFlag = os.O_RDONLY

	// Sub-commands

	argparser.AddCommand(inspectors)
	argparser.AddCommand(repairers)

	// Run

	if err := argparser.ExecuteContext(context.Background()); err != nil {
		textui.Fprintf(os.Stderr, "%v: error: %v\n", argparser.CommandPath(), err)
		os.Exit(1)
	}
}

func run(runE func(*cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		logger := textui.NewLogger(os.Stderr, globalFlags.logLevel.Level)
		ctx = dlog.WithLogger(ctx, logger)
		if globalFlags.logLevel.Level >= dlog.LogLevelDebug {
			ctx = dlog.WithField(ctx, "mem", new(textui.LiveMemUse))
		}
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

			defer func() {
				maybeSetErr(globalFlags.stopProfiling())
			}()
			cmd.SetContext(ctx)
			return runE(cmd, args)
		})
		return grp.Wait()
	}
}

func runWithRawFS(runE func(*btrfs.FS, *cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return run(func(cmd *cobra.Command, args []string) (err error) {
		maybeSetErr := func(_err error) {
			if _err != nil && err == nil {
				err = _err
			}
		}

		if len(globalFlags.pvs) == 0 {
			// We do this here instead of calling argparser.MarkPersistentFlagRequired("pv") so that
			// it doesn't interfere with the `help` sub-command.
			return cliutil.FlagErrorFunc(cmd, fmt.Errorf("must specify 1 or more physical volumes with --pv"))
		}
		fs, err := btrfsutil.Open(cmd.Context(), globalFlags.openFlag, globalFlags.pvs...)
		if err != nil {
			return err
		}
		defer func() {
			maybeSetErr(fs.Close())
		}()

		if globalFlags.mappings != "" {
			mappingsJSON, err := readJSONFile[[]btrfsvol.Mapping](cmd.Context(), globalFlags.mappings)
			if err != nil {
				return err
			}
			for _, mapping := range mappingsJSON {
				if err := fs.LV.AddMapping(mapping); err != nil {
					return err
				}
			}
		}

		return runE(fs, cmd, args)
	})
}

func runWithReadableFS(runE func(btrfs.ReadableFS, *cobra.Command, []string) error) func(*cobra.Command, []string) error {
	return runWithRawFS(func(fs *btrfs.FS, cmd *cobra.Command, args []string) error {
		return runE(btrfsutil.NewOldRebuiltForrest(fs), cmd, args)
	})
}
