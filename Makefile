PACKAGES=$(shell find . -name '*.go' -print0 | xargs -0 -n1 dirname | sort --unique)

default: test build install

metalint: deadcode cyclo aligncheck defercheck structcheck lint errcheck

deadcode:
	@deadcode $(PACKAGES) 2>&1

quick:
	@go clean ./...
	@go install ./...

build: tools setup
	@go clean ./...
	@go build ./...

install: build
	@go install ./...

test: tools setup
	@go test ./...

quicktest:
	@go test ./...

setup:
	@gdm restore

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

.PHONY: default metalint deadcode cyclo aligncheck defercheck structcheck lint errcheck tools dev build test quicktest quick quicktest install
