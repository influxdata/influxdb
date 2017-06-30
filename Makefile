VERSION := $(shell git describe --exact-match --tags 2>/dev/null)
BRANCH := $(shell git rev-parse --abbrev-ref HEAD)
COMMIT := $(shell git rev-parse --short HEAD)
PACKAGES=$(shell find . -name '*.go' -print0 | xargs -0 -n1 dirname | sort --unique)

LDFLAGS := -X main.commit=$(COMMIT) -X main.branch=$(BRANCH)
ifdef VERSION
	LDFLAGS += -X main.version=$(VERSION)
endif

default: prepare build

build:
	go install -ldflags "$(LDFLAGS)" ./...

clean:
	go clean ./...

# Get dependencies and use gdm to checkout changesets
prepare:
	go get github.com/sparrc/gdm
	gdm restore

metalint: deadcode cyclo aligncheck defercheck structcheck lint errcheck

deadcode:
	@deadcode $(PACKAGES) 2>&1

cyclo:
	@gocyclo -over 10 $(PACKAGES)

aligncheck:
	@aligncheck $(PACKAGES)

defercheck:
	@defercheck $(PACKAGES)


structcheck:
	@structcheck $(PACKAGES)

lint:
	@for pkg in $(PACKAGES); do golint $$pkg; done

errcheck:
	@for pkg in $(PACKAGES); do \
	  errcheck -ignorepkg=bytes,fmt -ignore=":(Rollback|Close)" $$pkg \
	done

tools:
	go get github.com/remyoudompheng/go-misc/deadcode
	go get github.com/alecthomas/gocyclo
	go get github.com/opennota/check/...
	go get github.com/golang/lint/golint
	go get github.com/kisielk/errcheck
	go get github.com/sparrc/gdm

.PHONY: default,metalint,deadcode,cyclo,aligncheck,defercheck,structcheck,lint,errcheck,tools
