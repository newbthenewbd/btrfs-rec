#!/bin/bash
set -e
go build ./cmd/btrfs-rec
if ! test -s ../scratch/dump.scan-for-nodes.json; then
	time ./btrfs-rec --pv=../scratch/dump-zero.img inspect scan-for-nodes > ../scratch/dump.scan-for-nodes.json ||
		{ r=$?; rm -f ../scratch/dump.scan-for-nodes.json; exit $r; }
fi
