# Top level Makefile for the entire project
#
# This Makefile encodes the "go generate" prerequisites ensuring that the proper tooling is installed and
# that the generate steps are executed when their prerequisite files change.
#
# This Makefile follows a few conventions:
#
#    * All cmds must be added to this top level Makefile.
#    * All binaries are placed in ./bin, its recommended to add this directory to your PATH.
#    * Each package that has a need to run go generate, must have its own Makefile for that purpose.
#    * All recursive Makefiles must support the all and clean targets
#

# SUBDIRS are directories that have their own Makefile.
# It is required that all SUBDIRS have the `all` and `clean` targets.
SUBDIRS := http ui chronograf storage

export GOPATH=$(shell go env GOPATH)
export GOOS=$(shell go env GOOS)
export GOARCH=$(shell go env GOARCH)

ifeq ($(GOARCH), amd64)
	# Including the assets tag requires the UI to be built for compilation to succeed.
	# Don't force it for running tests.
	GO_TEST_TAGS :=
	GO_BUILD_TAGS := assets
else
	# noasm needed to avoid a panic in Flux for non-amd64.
	GO_TEST_TAGS := noasm
	GO_BUILD_TAGS := assets,noasm
endif

GO_TEST_ARGS := -tags '$(GO_TEST_TAGS)'
GO_BUILD_ARGS := -tags '$(GO_BUILD_TAGS)'

ifeq ($(OS), Windows_NT)
	VERSION := $(shell git describe --exact-match --tags 2>nil)
else
	VERSION := $(shell git describe --exact-match --tags 2>/dev/null)
endif
COMMIT := $(shell git rev-parse --short HEAD)

LDFLAGS := $(LDFLAGS) -X main.commit=$(COMMIT)
ifdef VERSION
	LDFLAGS += -X main.version=$(VERSION)
endif

# Allow for `go test` to be swapped out by other tooling, i.e. `gotestsum`
GO_TEST_CMD=go test
# Allow for a subset of tests to be specified.
GO_TEST_PATHS=./...

# Test vars can be used by all recursive Makefiles
export PKG_CONFIG:=$(PWD)/scripts/pkg-config.sh
export GO_BUILD=env GO111MODULE=on go build $(GO_BUILD_ARGS) -ldflags "$(LDFLAGS)"
export GO_BUILD_SM=env GO111MODULE=on go build $(GO_BUILD_ARGS) -ldflags "-s -w $(LDFLAGS)"
export GO_INSTALL=env GO111MODULE=on go install $(GO_BUILD_ARGS) -ldflags "$(LDFLAGS)"
export GO_TEST=env GOTRACEBACK=all GO111MODULE=on $(GO_TEST_CMD) $(GO_TEST_ARGS)
# Do not add GO111MODULE=on to the call to go generate so it doesn't pollute the environment.
export GO_GENERATE=go generate $(GO_BUILD_ARGS)
export GO_VET=env GO111MODULE=on go vet $(GO_TEST_ARGS)
export GO_RUN=env GO111MODULE=on go run $(GO_BUILD_ARGS)
export PATH := $(PWD)/bin/$(GOOS):$(PATH)


# All go source files
SOURCES := $(shell find . -name '*.go' -not -name '*_test.go') go.mod go.sum

# All go source files excluding the vendored sources.
SOURCES_NO_VENDOR := $(shell find . -path ./vendor -prune -o -name "*.go" -not -name '*_test.go' -print)

# All assets for chronograf
UISOURCES := $(shell find ui -type f -not \( -path ui/build/\* -o -path ui/node_modules/\* -o -path ui/.cache/\* -o -name Makefile -prune \) )

# All precanned dashboards
PRECANNED := $(shell find chronograf/canned -name '*.json')

# List of binary cmds to build
CMDS := \
	bin/$(GOOS)/influxd

all: $(SUBDIRS) generate $(CMDS)

# Target to build subdirs.
# Each subdirs must support the `all` target.
$(SUBDIRS):
	$(MAKE) -C $@ all

#
# Define targets for commands
#
bin/$(GOOS)/influxd: $(SOURCES)
	$(GO_BUILD) -o $@ ./cmd/$(shell basename "$@")

# Ease of use build for just the go binary
influxd: bin/$(GOOS)/influxd

#
# Define targets for the web ui
#

node_modules: ui/node_modules

# phony target to wait for server to be alive
ping:
	./etc/pinger.sh

e2e: ping
	make -C ui e2e

chronograf_lint:
	make -C ui lint

ui/node_modules:
	make -C ui node_modules

ui_client:
	make -C ui client

#
# Define action only targets
#

fmt: $(SOURCES_NO_VENDOR)
	gofmt -w -s $^

checkfmt:
	./etc/checkfmt.sh
	$(GO_RUN) github.com/editorconfig-checker/editorconfig-checker/cmd/editorconfig-checker

tidy:
	GO111MODULE=on go mod tidy

checktidy:
	./etc/checktidy.sh

checkgenerate:
	./etc/checkgenerate.sh

generate: $(SUBDIRS)

test-js: node_modules
	make -C ui test

test-go:
	$(GO_TEST) $(GO_TEST_PATHS)

test-flux:
	@./etc/test-flux.sh

test-tls:
	@./etc/test-tls.sh

test-influxql-integration:
	$(GO_TEST) -mod=readonly ./influxql/_v1tests

test-influxql-validation:
	$(GO_TEST) -mod=readonly ./influxql/_v1validation

test-integration: GO_TAGS=integration
test-integration:
	$(GO_TEST) -count=1 $(GO_TEST_PATHS)

test: test-go test-js

test-go-race:
	$(GO_TEST) -v -race -count=1 $(GO_TEST_PATHS)

vet:
	$(GO_VET) -v ./...

bench:
	$(GO_TEST) -bench=. -run=^$$ ./...

build: all

pkg-config:
	go build -o $(GOPATH)/bin/pkg-config github.com/influxdata/pkg-config

clean:
	@for d in $(SUBDIRS); do $(MAKE) -C $$d clean; done
	$(RM) -r bin
	$(RM) -r dist

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
chronogiraffe: $(SUBDIRS) generate $(CMDS)
	@echo "$$CHRONOGIRAFFE"

run: chronogiraffe
	./bin/$(GOOS)/influxd --assets-path=ui/build

run-e2e: chronogiraffe
	./bin/$(GOOS)/influxd --assets-path=ui/build --e2e-testing --store=memory

# generate feature flags
flags:
	$(GO_GENERATE) ./kit/feature

docker-image-influx:
	@cp .gitignore .dockerignore
	@docker image build -t influxdb:dev --target influx .

docker-image-ui:
	@cp .gitignore .dockerignore
	@docker image build -t influxui:dev --target ui .
	
dshell-image:
	@cp .gitignore .dockerignore
	@docker image build --build-arg "USERID=$(shell id -u)" -t influxdb:dshell --target dshell .

dshell: dshell-image
	@docker container run --rm -p 8086:8086 -p 8080:8080 -u $(shell id -u) -it -v $(shell pwd):/code -w /code influxdb:dshell 

# .PHONY targets represent actions that do not create an actual file.
.PHONY: all $(SUBDIRS) run fmt checkfmt tidy checktidy checkgenerate test test-go test-js test-go-race test-tls bench clean node_modules vet nightly chronogiraffe dist ping protoc e2e run-e2e influxd libflux flags dshell dclean docker-image-flux docker-image-influx pkg-config
