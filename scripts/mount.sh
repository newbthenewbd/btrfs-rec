#!/bin/bash
set -e
set -x

go build ./cmd/btrfs-rec

mkdir -p ../scratch/dump-zero.mnt

sudo ./btrfs-rec \
     --pv ../scratch/dump-zero.img \
     --mappings=../scratch/dump-zero.gen/2.mappings.json \
     inspect mount \
     --skip-filesums \
     ../scratch/dump-zero.mnt
