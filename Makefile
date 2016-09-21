VERSION ?= $$(git describe --always --tags)
COMMIT ?= $$(git rev-parse --short=8 HEAD)
BRANCH ?= $$(git rev-parse --abbrev-ref HEAD | tr / _)
BUILD_TIME ?= $$(date +%FT%T%z)

SOURCES := $(shell find . -name '*.go')

LDFLAGS=-ldflags "-s -X main.Version=${VERSION} -X main.Commit=${COMMIT} -X main.BuildTime=${BUILD_TIME}  -X main.Branch=${BRANCH}"
BINARY=mrfusion

default: dep build

build: assets ${BINARY}

dev: dev-assets ${BINARY}

${BINARY}: $(SOURCES)
	go build -o ${BINARY} ${LDFLAGS} ./cmd/mr-fusion-server/main.go

docker-${BINARY}: $(SOURCES)
	CGO_ENABLED=0 GOOS=linux go build -installsuffix cgo -o ${BINARY} ${LDFLAGS} \
		./cmd/mr-fusion-server/main.go

docker: dep assets docker-${BINARY}
	docker build -t mrfusion .

assets: js bindata

dev-assets: dev-js dev-bindata

bindata:
	go-bindata -o dist/dist_gen.go -ignore 'map|go' -pkg dist ui/build/...

dev-bindata:
	go-bindata -debug -o dist/dist_gen.go -ignore 'map|go' -pkg dist ui/build/...

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

test: jstest gotest

gotest:
	go test -race ./...

jstest:
	cd ui && npm test

run: ${BINARY}
	./mrfusion --port 8888

run-dev: ${BINARY}
	./mrfusion -d --port 8888

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
	cd ui && npm run clean

.PHONY: clean test jstest gotest run
