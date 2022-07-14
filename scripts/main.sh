#!/bin/bash
set -ex
go build ./cmd/btrfs-rec
if ! test -s ../scratch/dump.scan-for-nodes.json; then
	time ./btrfs-rec --pv=../scratch/dump-zero.img inspect scan-for-nodes > ../scratch/dump.scan-for-nodes.json
fi
if ! test -s ../scratch/dump.rebuilt-mappings.json; then
	time ./btrfs-rec --pv=../scratch/dump-zero.img inspect rebuild-mappings ../scratch/dump.scan-for-nodes.json \
	     > ../scratch/dump.rebuilt-mappings.json \
	     2> >(tee >&2 ../scratch/dump.rebuilt-mappings.log)
fi
time ./btrfs-rec --pv=../scratch/dump-zero.img --mappings=../scratch/dump.rebuilt-mappings.json inspect ls-files \
     &> ../scratch/dump.ls-files.txt
