VERSION ?= $$(git describe --always --tags)
COMMIT ?= $$(git rev-parse --short=8 HEAD)
BRANCH ?= $$(git rev-parse --abbrev-ref HEAD | tr / _)
BUILD_TIME ?= $$(date +%FT%T%z)

SOURCES := $(shell find . -name '*.go')

LDFLAGS=-ldflags "-s -X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildTime=${BUILD_TIME}  -X main.Branch=${BRANCH}"
BINARY=mrfusion

default: prepare ${BINARY}

${BINARY}: $(SOURCES)
	go build -o ${BINARY} ${LDFLAGS} ./cmd/mr-fusion-server/main.go

docker-${BINARY}: $(SOURCES)
	CGO_ENABLED=0 GOOS=linux go build -installsuffix cgo -o ${BINARY} ${LDFLAGS} \
		./cmd/mr-fusion-server/main.go

assets:
	mkdir -p ui/build
	go-bindata -o ui/ui.go -ignore 'map|go' -pkg ui -nocompress=true ui/build/...

dev:
	go get github.com/sparrc/gdm
	gdm restore
	go get -u github.com/jteeuwen/go-bindata/...

prepare: dev assets

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi

test:
	go test -race ./...

.PHONY: clean test
