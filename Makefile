# Copyright (C) 2022  Luke Shumaker <lukeshu@lukeshu.com>
#
# SPDX-License-Identifier: GPL-2.0-or-later

# main

build:
	go build -o bin/ ./cmd/...
.PHONY: build

check:
	go test -race ./...
.PHONY: check

lint: tools/bin/golangci-lint
	tools/bin/golangci-lint run ./...
.PHONY: lint

# generate

generate/files  = COPYING.gpl-2.0.txt
generate/files += COPYING.gpl-3.0.txt
generate/files += COPYING.apache-2.0.txt

generate: generate-clean
	$(MAKE) -C lib/btrfs
	$(MAKE) $(generate/files)
.PHONY: generate

generate-clean:
	$(MAKE) -C lib/btrfs clean
	rm -f $(generate/files)
.PHONY: generate-clean

COPYING.gpl-2.0.txt:
	curl https://www.gnu.org/licenses/old-licenses/gpl-2.0.txt > $@
COPYING.gpl-3.0.txt:
	curl https://www.gnu.org/licenses/gpl-3.0.txt > $@
COPYING.apache-2.0.txt:
	curl https://apache.org/licenses/LICENSE-2.0.txt > $@

# tools

tools/bin/%: tools/src/%/pin.go tools/src/%/go.mod
	cd $(<D) && GOOS= GOARCH= go build -o $(abspath $@) $$(sed -En 's,^import "(.*)".*,\1,p' pin.go)

# go mod tidy

goversion = 1.19

go-mod-tidy:
.PHONY: go-mod-tidy

go-mod-tidy: go-mod-tidy/main
go-mod-tidy/main:
	rm -f go.sum
	go mod tidy -go $(goversion) -compat $(goversion)
.PHONY: go-mod-tidy/main

go-mod-tidy: $(patsubst tools/src/%/go.mod,go-mod-tidy/tools/%,$(wildcard tools/src/*/go.mod))
go-mod-tidy/tools/%: tools/src/%/go.mod
	rm -f tools/src/$*/go.sum
	cd tools/src/$* && go mod tidy -go $(goversion) -compat $(goversion)
.PHONY: go-mod-tidy/tools/%
