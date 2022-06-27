#!/bin/bash
diff -u \
     <(../btrfs-progs/btrfs inspect dump-tree --noscan ../scratch/new.img | sed -e 's/ UNKNOWN.0 / UNTYPED /g' -e 's/\b18446744073709551615\b/-1/g')  \
     <(go run ./cmd/btrfs-dump-tree/ ../scratch/new.img)
