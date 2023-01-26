// Copyright (C) 2022-2023  Luke Shumaker <lukeshu@lukeshu.com>
//
// SPDX-License-Identifier: GPL-2.0-or-later

module git.lukeshu.com/btrfs-progs-ng

go 1.19

require (
	git.lukeshu.com/go/lowmemjson v0.2.0
	github.com/datawire/dlib v1.3.0
	github.com/datawire/ocibuild v0.0.3-0.20220423003204-fc6a4e9f90dc
	github.com/davecgh/go-spew v1.1.1
	github.com/hashicorp/golang-lru v0.5.4
	github.com/jacobsa/fuse v0.0.0-20220702091825-13117049f383
	github.com/spf13/cobra v1.5.0
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.0
	golang.org/x/exp v0.0.0-20220518171630-0b5c67f07fdf
	golang.org/x/text v0.3.7
)

require (
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/sirupsen/logrus v1.8.1 // indirect
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a // indirect
	golang.org/x/term v0.0.0-20201126162022-7de9c90e9dd1 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/jacobsa/fuse => github.com/lukeshu/jacobsa-fuse v0.0.0-20220706162300-f42bfdd0fc53
