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

SUBDIRS := query task

GO_ARGS=-tags '$(GO_TAGS)'

# Test vars can be used by all recursive Makefiles
export GOOS=$(shell go env GOOS)
export GO_BUILD=env GO111MODULE=on go build $(GO_ARGS)
export GO_TEST=env GO111MODULE=on go test $(GO_ARGS)
# Do not add GO111MODULE=on to the call to go generate so it doesn't pollute the environment.
export GO_GENERATE=go generate $(GO_ARGS)
export GO_VET=env GO111MODULE=on go vet $(GO_ARGS)
export PATH := $(PWD)/bin/$(GOOS):$(PATH)


# All go source files
SOURCES := $(shell find . -name '*.go' -not -name '*_test.go')

# All go source files excluding the vendored sources.
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)

# All assets for chronograf
UISOURCES := $(shell find ui -type f -not \( -path ui/build/\* -o -path ui/node_modules/\* -o -path ui/.cache/\* -o -name Makefile -prune \) )

# All precanned dashboards
PRECANNED := $(shell find chronograf/canned -name '*.json')


# List of binary cmds to build
CMDS := \
	bin/$(GOOS)/influx \
	bin/$(GOOS)/influxd

# Default target to build all go commands.
#
# This target sets up the dependencies to correctly build all go commands.
# Other targets must depend on this target to correctly builds CMDS.
all: GO_ARGS=-tags 'assets $(GO_TAGS)'
all: node_modules subdirs ui generate $(CMDS)

# Target to build subdirs.
# Each subdirs must support the `all` target.
subdirs: $(SUBDIRS)
	@for d in $^; do $(MAKE) -C $$d all; done


ui:
	$(MAKE) -C ui all

#
# Define targets for commands
#
$(CMDS): $(SOURCES)
	$(GO_BUILD) -o $@ ./cmd/$(shell basename "$@")

#
# Define targets for the web ui
#

node_modules: ui/node_modules

chronograf_lint:
	make -C ui lint

ui/node_modules:
	make -C ui node_modules

ui/build:
	mkdir -p ui/build

#
# Define action only targets
#

fmt: $(SOURCES_NO_VENDOR)
	gofmt -w -s $^

checkfmt:
	./etc/checkfmt.sh

tidy:
	GO111MODULE=on go mod tidy

checktidy:
	./etc/checktidy.sh

chronograf/dist/dist_gen.go: ui/build $(UISOURCES)
	 $(GO_GENERATE) ./chronograf/dist/...

chronograf/server/swagger_gen.go: chronograf/server/swagger.json
	 $(GO_GENERATE) ./chronograf/server/...

chronograf/canned/bin_gen.go: $(PRECANNED)
	 $(GO_GENERATE) ./chronograf/canned/...

generate: chronograf/dist/dist_gen.go chronograf/server/swagger_gen.go chronograf/canned/bin_gen.go

test-js: node_modules
	make -C ui test

test-go:
	$(GO_TEST) ./...

test: test-go test-js

test-go-race:
	$(GO_TEST) -race -count=1 ./...

vet:
	$(GO_VET) -v ./...

bench:
	$(GO_TEST) -bench=. -run=^$$ ./...

nightly: all
	env GO111MODULE=on go run github.com/goreleaser/goreleaser --snapshot --rm-dist --publish-snapshots

# Recursively clean all subdirs
clean: $(SUBDIRS)
	@for d in $^; do $(MAKE) -C $$d $(MAKECMDGOALS); done
	$(MAKE) -C ui $(MAKECMDGOALS)
	rm -rf bin


define CHRONOGIRAFFE
             ._ o o
             \_`-)|_
          ,""      _\_
        ,"  ## |   0 0.
      ," ##   ,-\__    `.
    ,"       /     `--._;) - "HAI, I'm Chronogiraffe. Let's be friends!"
  ,"     ## /
,"   ##    /
endef
export CHRONOGIRAFFE
chronogiraffe: subdirs generate $(CMDS)
	@echo "$$CHRONOGIRAFFE"

run: chronogiraffe
	./bin/$(GOOS)/influxd --developer-mode=true


# .PHONY targets represent actions that do not create an actual file.
.PHONY: all subdirs $(SUBDIRS) ui run fmt checkfmt tidy checktidy test test-go test-js test-go-race bench clean node_modules vet nightly chronogiraffe
