#!/bin/bash
set -ex
go build ./cmd/btrfs-rec
if ! test -s ../scratch/dump-zero.scan-for-nodes.json; then
	time ./btrfs-rec --pv=../scratch/dump-zero.img inspect scan-for-nodes > ../scratch/dump-zero.scan-for-nodes.json
fi
if ! test -s ../scratch/dump-zero.mappings.0.json; then
	time ./btrfs-rec --pv=../scratch/dump-zero.img inspect rebuild-mappings ../scratch/dump-zero.scan-for-nodes.json \
	     > ../scratch/dump-zero.mappings.0.json \
	     2> >(tee >&2 ../scratch/dump-zero.mappings.0.log)
fi
if ! test -s ../scratch/dump-zero.mappings.1.json; then
	time ./btrfs-rec --pv=../scratch/dump-zero.img --mappings=../scratch/dump-zero.mappings.0.json inspect scan-for-extents ../scratch/dump-zero.scan-for-nodes.json \
	     > ../scratch/dump-zero.mappings.1.json \
	     2> >(tee >&2 ../scratch/dump-zero.mappings.1.log)
fi


#time ./btrfs-rec --pv=../scratch/dump-zero.img --mappings=../scratch/dump-zero.mappings.0.json inspect ls-files \
#     &> ../scratch/dump-zero.ls-files.txt
