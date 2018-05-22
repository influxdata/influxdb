VERSION ?= $(shell git describe --always --tags)
SUBDIRS := ast parser promql
GO_ARGS=-tags '$(GO_TAGS)'
export GO_BUILD=go build $(GO_ARGS)
export GO_TEST=go test $(GO_ARGS)
export GO_GENERATE=go generate $(GO_ARGS)

SOURCES := $(shell find . -name '*.go' -not -name '*_test.go')
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)

all: Gopkg.lock $(SUBDIRS) bin/platform/query bin/ifqld

$(SUBDIRS): bin/pigeon bin/cmpgen
	$(MAKE) -C $@ $(MAKECMDGOALS)

bin/platform/query: $(SOURCES) bin/pigeon bin/cmpgen
	$(GO_BUILD) -i -o bin/platform/query ./cmd/ifql

bin/platform/queryd: $(SOURCES) bin/pigeon bin/cmpgen
	$(GO_BUILD) -i -o bin/platform/queryd ./cmd/ifqld

bin/pigeon: ./vendor/github.com/mna/pigeon/main.go
	go build -i -o bin/pigeon  ./vendor/github.com/mna/pigeon

bin/cmpgen: ./ast/asttest/cmpgen/main.go
	go build -i -o bin/cmpgen ./ast/asttest/cmpgen

Gopkg.lock: Gopkg.toml
	dep ensure -v

vendor/github.com/mna/pigeon/main.go: Gopkg.lock
	dep ensure -v

fmt: $(SOURCES_NO_VENDOR)
	goimports -w $^

update:
	dep ensure -v -update

test: Gopkg.lock bin/platform/query
	$(GO_TEST) ./...

test-race: Gopkg.lock bin/platform/query
	$(GO_TEST) -race ./...

bench: Gopkg.lock bin/platform/query
	$(GO_TEST) -bench=. -run=^$$ ./...

bin/goreleaser:
	go build -i -o bin/goreleaser ./vendor/github.com/goreleaser/goreleaser

dist: bin/goreleaser
	PATH=./bin:${PATH} goreleaser --rm-dist --release-notes CHANGELOG.md

release: dist release-docker

release-docker:
	docker build -t quay.io/influxdb/platform/queryd:latest .
	docker tag quay.io/influxdb/platform/queryd:latest quay.io/influxdb/ifqld:${VERSION}
	docker push quay.io/influxdb/platform/queryd:latest
	docker push quay.io/influxdb/platform/queryd:${VERSION}

clean: $(SUBDIRS)
	rm -rf bin dist

.PHONY: all clean $(SUBDIRS) update test test-race bench release docker dist fmt
