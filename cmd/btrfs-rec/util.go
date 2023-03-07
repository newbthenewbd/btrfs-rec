// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

package main

import (
	"bufio"
	"context"
	"io"
	"os"

	"git.lukeshu.com/go/lowmemjson"
	"github.com/datawire/dlib/dlog"

	"git.lukeshu.com/btrfs-progs-ng/cmd/btrfs-rec/inspect/rebuildmappings"
	"git.lukeshu.com/btrfs-progs-ng/lib/btrfs/btrfsvol"
	"git.lukeshu.com/btrfs-progs-ng/lib/containers"
	"git.lukeshu.com/btrfs-progs-ng/lib/maps"
	"git.lukeshu.com/btrfs-progs-ng/lib/streamio"
)

func readJSONFile[T any](ctx context.Context, filename string) (T, error) {
	fh, err := os.Open(filename)
	if err != nil {
		var zero T
		return zero, err
	}
	buf, err := streamio.NewRuneScanner(dlog.WithField(ctx, "btrfs.read-json-file", filename), fh)
	defer func() {
		_ = buf.Close()
	}()
	if err != nil {
		var zero T
		return zero, err
	}
	var ret T
	if err := lowmemjson.NewDecoder(buf).DecodeThenEOF(&ret); err != nil {
		var zero T
		return zero, err
	}
	return ret, nil
}

func writeJSONFile(w io.Writer, obj any, cfg lowmemjson.ReEncoderConfig) (err error) {
	buffer := bufio.NewWriter(w)
	defer func() {
		if _err := buffer.Flush(); err == nil && _err != nil {
			err = _err
		}
	}()
	return lowmemjson.NewEncoder(lowmemjson.NewReEncoder(buffer, cfg)).Encode(obj)
}

func readNodeList(ctx context.Context, filename string) ([]btrfsvol.LogicalAddr, error) {
	if filename == "" {
		return nil, nil
	}
	results, err := readJSONFile[rebuildmappings.ScanDevicesResult](ctx, filename)
	if err != nil {
		return nil, err
	}
	var cnt int
	for _, devResults := range results {
		cnt += len(devResults.FoundNodes)
	}
	set := make(containers.Set[btrfsvol.LogicalAddr], cnt)
	for _, devResults := range results {
		for laddr := range devResults.FoundNodes {
			set.Insert(laddr)
		}
	}
	return maps.SortedKeys(set), nil
}
