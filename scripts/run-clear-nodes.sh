#!/bin/bash
set -e
rm -f ../scratch/dump-clearnodes.img
cp --reflink=always ../scratch/dump-{scratch,clearnodes}.img
time go run ./cmd/btrfs-rec repair clear-bad-nodes --pv=../scratch/dump-clearnodes.img
