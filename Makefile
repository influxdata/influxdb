# Top level Makefile for the entire project
#
# This Makefile encodes the "go generate" prerequisites ensuring that the proper tooling is installed and
# that the generate steps are executed when their prerequisite files change.
#
# This Makefile follows a few conventions:
#
#    * All cmds must be added to this top level Makefile.
#    * All binaries are placed in ./bin, its recommended to add this directory to your PATH.
#
export GOPATH=$(shell go env GOPATH)
export GOOS=$(shell go env GOOS)
export GOARCH=$(shell go env GOARCH)

ifneq (,$(filter $(GOARCH),amd64 s390x))
	# Including the assets tag requires the UI to be built for compilation to succeed.
	# Don't force it for running tests.
	GO_TEST_TAGS :=
	GO_BUILD_TAGS := assets
else
	# noasm needed to avoid a panic in Flux for non-amd64, non-s390x.
	GO_TEST_TAGS := noasm
	GO_BUILD_TAGS := assets,noasm
endif

# Tags used for builds and tests on all architectures
COMMON_TAGS := sqlite_foreign_keys,sqlite_json

GO_TEST_ARGS := -tags '$(COMMON_TAGS),$(GO_TEST_TAGS)'
GO_BUILD_ARGS := -tags '$(COMMON_TAGS),$(GO_BUILD_TAGS)'

# Use default flags, but allow adding -gcflags "..." if desired. Eg, for debug
# builds, may want to use GCFLAGS="all=-N -l" in the build environment.
GCFLAGS ?=
ifneq ($(GCFLAGS),)
GO_BUILD_ARGS += -gcflags "$(GCFLAGS)"
endif

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

# List of binary cmds to build
CMDS := \
	bin/$(GOOS)/influxd

all: generate $(CMDS)

#
# Define targets for commands
#
bin/$(GOOS)/influxd: $(SOURCES)
	$(GO_BUILD) -o $@ ./cmd/$(shell basename "$@")

influxd: bin/$(GOOS)/influxd

static/data/build: scripts/fetch-ui-assets.sh
	./scripts/fetch-ui-assets.sh

static/data/swagger.json: scripts/fetch-swagger.sh
	./scripts/fetch-swagger.sh

# static/static_gen.go is the output of go-bindata, embedding all assets used by the UI.
static/static_gen.go: static/data/build static/data/swagger.json
	$(GO_GENERATE) ./static

#
# Define action only targets
#

fmt: $(SOURCES_NO_VENDOR)
	./etc/fmt.sh

checkfmt:
	./etc/checkfmt.sh
	$(GO_RUN) github.com/editorconfig-checker/editorconfig-checker/cmd/editorconfig-checker

tidy:
	GO111MODULE=on go mod tidy

checktidy:
	./etc/checktidy.sh

checkgenerate:
	./etc/checkgenerate.sh

checksqlmigrations:
	./etc/check-sql-migrations.sh

# generate-web-assets outputs all the files needed to link the UI to the back-end.
# Currently, none of these files are tracked by git.
generate-web-assets: static/static_gen.go

# generate-sources outputs all the Go files generated from protobufs, tmpls, and other tooling.
# These files are tracked by git; CI will enforce that they are up-to-date.
generate-sources: protoc tmpl stringer goimports
	$(GO_GENERATE) ./influxql/... ./models/... ./pkg/... ./storage/... ./tsdb/... ./v1/...

generate: generate-web-assets generate-sources

protoc:
	$(GO_INSTALL) google.golang.org/protobuf/cmd/protoc-gen-go@v1.27.1

tmpl:
	$(GO_INSTALL) github.com/benbjohnson/tmpl

stringer:
	$(GO_INSTALL) golang.org/x/tools/cmd/stringer

goimports:
	$(GO_INSTALL) golang.org/x/tools/cmd/goimports

test-go:
	$(GO_TEST) $(GO_TEST_PATHS)

test-flux:
	@./etc/test-flux.sh

test-tls:
	@./etc/test-tls.sh

test-integration: GO_TAGS=integration
test-integration:
	$(GO_TEST) -count=1 $(GO_TEST_PATHS)

test: test-go

test-go-race:
	$(GO_TEST) -v -race -count=1 $(GO_TEST_PATHS)

vet:
	$(GO_VET) -v ./...

bench:
	$(GO_TEST) -bench=. -run=^$$ ./...

build: all

pkg-config:
	$(GO_INSTALL) github.com/influxdata/pkg-config

clean:
	$(RM) -r static/static_gen.go static/data
	$(RM) -r bin
	$(RM) -r dist

# generate feature flags
flags:
	$(GO_GENERATE) ./kit/feature

docker-image-influx:
	@cp .gitignore .dockerignore
	@docker image build -t influxdb:dev --target influx .
	
dshell-image:
	@cp .gitignore .dockerignore
	@docker image build --build-arg "USERID=$(shell id -u)" -t influxdb:dshell --target dshell .

dshell: dshell-image
	@docker container run --rm -p 8086:8086 -p 8080:8080 -u $(shell id -u) -it -v $(shell pwd):/code -w /code influxdb:dshell 

# .PHONY targets represent actions that do not create an actual file.
.PHONY: all $(SUBDIRS) run fmt checkfmt tidy checktidy checkgenerate test test-go test-go-race test-tls bench clean node_modules vet nightly dist protoc influxd libflux flags dshell dclean docker-image-flux docker-image-influx pkg-config
