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

generate:
	$(MAKE) -C lib/btrfs
.PHONY: generate

generate-clean:
	$(MAKE) -C lib/btrfs clean
.PHONY: generate-clean

# tools

tools/bin/%: tools/src/%/pin.go tools/src/%/go.mod
	cd $(<D) && GOOS= GOARCH= go build -o $(abspath $@) $$(sed -En 's,^import "(.*)".*,\1,p' pin.go)

# go mod tidy

goversion = 1.18

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
