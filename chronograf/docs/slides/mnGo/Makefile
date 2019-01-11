.PHONY: assets dep clean test gotest gotestrace jstest run run-dev ctags continuous

VERSION ?= $(shell git describe --always --tags)
COMMIT ?= $(shell git rev-parse --short=8 HEAD)
GDM := $(shell command -v gdm 2> /dev/null)
GOBINDATA := $(shell go list -f {{.Root}}  github.com/kevinburke/go-bindata 2> /dev/null)

SOURCES := $(shell find . -name '*.go' ! -name '*_gen.go')
UISOURCES := $(shell find ui -type f -not \( -path ui/build/\* -o -path ui/node_modules/\* -prune \) )

LDFLAGS=-ldflags "-s -X main.version=${VERSION} -X main.commit=${COMMIT}"
BINARY=chronograf

.DEFAULT_GOAL := all

all: dep build

build: assets ${BINARY}

dev: dep dev-assets ${BINARY}

${BINARY}: $(SOURCES) .bindata .jsdep .godep
	go build -o ${BINARY} ${LDFLAGS} ./cmd/chronograf/main.go

docker-${BINARY}: $(SOURCES)
	CGO_ENABLED=0 GOOS=linux go build -installsuffix cgo -o ${BINARY} ${LDFLAGS} \
		./cmd/chronograf/main.go

docker: dep assets docker-${BINARY}
	docker build -t chronograf .

assets: .jssrc .bindata

dev-assets: .dev-jssrc .bindata

.bindata: server/swagger_gen.go canned/bin_gen.go dist/dist_gen.go
	@touch .bindata

dist/dist_gen.go: $(UISOURCES)
	go generate -x ./dist

server/swagger_gen.go: server/swagger.json
	go generate -x ./server

canned/bin_gen.go: canned/*.json
	go generate -x ./canned

.jssrc: $(UISOURCES)
	cd ui && npm run build
	@touch .jssrc

.dev-jssrc: $(UISOURCES)
	cd ui && npm run build:dev
	@touch .dev-jssrc

dep: .jsdep .godep

.godep: Godeps
ifndef GDM
	@echo "Installing GDM"
	go get github.com/sparrc/gdm
endif
ifndef GOBINDATA
	@echo "Installing go-bindata"
	go get -u github.com/kevinburke/go-bindata/...
endif
	gdm restore
	@touch .godep

.jsdep: ui/package-lock.json
	cd ui && npm i
	@touch .jsdep

gen: bolt/internal/internal.proto
	go generate -x ./bolt/internal

test: jstest gotest gotestrace

gotest:
	go test ./...

gotestrace:
	go test -race ./...

jstest:
	cd ui && npm test

run: ${BINARY}
	./chronograf

run-dev: ${BINARY}
	./chronograf -d --log-level=debug

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
	cd ui && npm run clean
	cd ui && rm -rf node_modules
	rm -f dist/dist_gen.go canned/bin_gen.go server/swagger_gen.go
	@rm -f .godep .jsdep .jssrc .dev-jssrc .bindata

continuous:
	while true; do if fswatch -r --one-event .; then echo "#-> Starting build: `date`"; make dev; pkill chronograf; ./chronograf -d --log-level=debug & echo "#-> Build complete."; fi; sleep 0.5; done

ctags:
	ctags -R --languages="Go" --exclude=.git --exclude=ui .
