# Top level Makefile for the entire project
#
# This Makefile encodes the "go generate" prerequeisites ensuring that the proper tooling is installed and
# that the generate steps are executed when their prerequeisites files change.
#
# This Makefile follows a few conventions:
#
#    * All cmds must be added to this top level Makefile.
#    * All binaries are placed in ./bin, its recommended to add this directory to your PATH.
#    * Each package that has a need to run go generate, must have its own Makefile for that purpose.
#    * All recursive Makefiles must support the targets: all and clean.
#

SUBDIRS := query

GO_ARGS=-tags '$(GO_TAGS)'

# Test vars can be used by all recursive Makefiles
export GOOS=$(shell go env GOOS)
export GO_BUILD=go build $(GO_ARGS)
export GO_TEST=go test $(GO_ARGS)
export GO_GENERATE=go generate $(GO_ARGS)
export GO_VET= go vet $(GO_ARGS)


# All go source files
SOURCES := $(shell find . -name '*.go' -not -name '*_test.go')

# All go source files excluding the vendored sources.
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)

# List of binary cmds to build
CMDS := \
	bin/$(GOOS)/influx \
	bin/$(GOOS)/idpd \
	bin/$(GOOS)/fluxd \
	bin/$(GOOS)/transpilerd

# List of utilities to build as part of the build process
UTILS := \
	bin/$(GOOS)/pigeon \
	bin/$(GOOS)/cmpgen \
	bin/$(GOOS)/goreleaser

# Default target to build all commands.
#
# This target setups the dependencies to correctly build all commands.
# Other targets must depend on this target to correctly builds CMDS.
all: Gopkg.lock $(UTILS) subdirs $(CMDS)

# Target to build subdirs.
# Each subdirs must support the `all` target.
subdirs: $(SUBDIRS)
	@for d in $^; do $(MAKE) -C $$d all; done

#
# Define targets for commands
#
$(CMDS): $(SOURCES)
	$(GO_BUILD) -i -o $@ ./cmd/$(shell basename "$@")

#
# Define targets for utilities
#

bin/$(GOOS)/pigeon: ./vendor/github.com/mna/pigeon/main.go
	go build -i -o $@  ./vendor/github.com/mna/pigeon

bin/$(GOOS)/cmpgen: ./query/ast/asttest/cmpgen/main.go
	go build -i -o $@ ./query/ast/asttest/cmpgen

bin/$(GOOS)/goreleaser: ./vendor/github.com/goreleaser/goreleaser/main.go
	go build -i -o $@ ./vendor/github.com/goreleaser/goreleaser

#
# Define how source dependencies are managed
#

Gopkg.lock: Gopkg.toml
	dep ensure -v
	touch Gopkg.lock

vendor/github.com/mna/pigeon/main.go: Gopkg.lock
	dep ensure -v -vendor-only

vendor/github.com/goreleaser/goreleaser/main.go: Gopkg.lock
	dep ensure -v -vendor-only

#
# Define action only targets
#

fmt: $(SOURCES_NO_VENDOR)
	goimports -w $^

test: all
	$(GO_TEST) ./...

test-race: all
	$(GO_TEST) -race ./...

vet: all
	$(GO_VET) -v ./...

bench: all
	$(GO_TEST) -bench=. -run=^$$ ./...

nightly: bin/$(GOOS)/goreleaser all
	PATH=./bin/$(GOOS):${PATH} goreleaser --snapshot --rm-dist

# Recursively clean all subdirs
clean: $(SUBDIRS)
	@for d in $^; do $(MAKE) -C $$d $(MAKECMDGOALS); done
	rm -rf bin

# .PHONY targets represent actions that do not create an actual file.
.PHONY: all subdirs $(SUBDIRS) fmt test test-race bench clean
