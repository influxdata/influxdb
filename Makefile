VERSION ?= $(shell git describe --always --tags)
COMMIT ?= $(shell git rev-parse --short=8 HEAD)

SOURCES := $(shell find . -name '*.go')

LDFLAGS=-ldflags "-s -X main.Version=${VERSION} -X main.Commit=${COMMIT}"
BINARY=chronograf

default: dep build

build: assets ${BINARY}

dev: dev-assets ${BINARY}

${BINARY}: $(SOURCES)
	go build -o ${BINARY} ${LDFLAGS} ./cmd/chronograf/main.go

docker-${BINARY}: $(SOURCES)
	CGO_ENABLED=0 GOOS=linux go build -installsuffix cgo -o ${BINARY} ${LDFLAGS} \
		./cmd/chronograf/main.go

docker: dep assets docker-${BINARY}
	docker build -t chronograf .

assets: js bindata

dev-assets: dev-js bindata

bindata:
	go generate -x ./dist
	go generate -x ./server

js:
	cd ui && npm run build

dev-js:
	cd ui && npm run build:dev

dep: jsdep godep

godep:
	go get github.com/sparrc/gdm
	gdm restore
	go get -u github.com/jteeuwen/go-bindata/...

jsdep:
	cd ui && npm install

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
	./chronograf --port 8888

run-dev: ${BINARY}
	./chronograf -d --port 8888

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
	cd ui && npm run clean

.PHONY: clean test jstest gotest run
