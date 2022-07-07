#!/bin/bash
set -e
rm -f ../scratch/dump-check.img
cp --reflink=always ../scratch/dump-{clearnodes,check}.img
make -C ../btrfs-progs
time ../btrfs-progs/btrfs check --progress --repair ../scratch/dump-check.img
