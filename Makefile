.PHONY: assets dep clean test gotest gotestrace jstest run run-dev run-hmr ctags

VERSION ?= $(shell git describe --always --tags)
COMMIT ?= $(shell git rev-parse --short=8 HEAD)
GOBINDATA := $(shell go list -f {{.Root}}  github.com/jteeuwen/go-bindata 2> /dev/null)
YARN := $(shell command -v yarn 2> /dev/null)

SOURCES := $(shell find . -name '*.go' ! -name '*_gen.go' -not -path "./vendor/*" )
UISOURCES := $(shell find ui -type f -not \( -path ui/build/\* -o -path ui/node_modules/\* -prune \) )

unexport LDFLAGS
LDFLAGS=-ldflags "-s -X main.version=${VERSION} -X main.commit=${COMMIT}"
BINARY=chronograf

.DEFAULT_GOAL := all

all: dep build

build: assets ${BINARY}

dev: dep dev-assets ${BINARY}

${BINARY}: $(SOURCES) .bindata .jsdep .godep
	go build -o ${BINARY} ${LDFLAGS} ./cmd/chronograf/main.go

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
chronogiraffe: ${BINARY}
	@echo "$$CHRONOGIRAFFE"

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
	cd ui && yarn run build
	@touch .jssrc

.dev-jssrc: $(UISOURCES)
	cd ui && yarn run build:dev
	@touch .dev-jssrc

dep: .jsdep .godep

.godep:
ifndef GOBINDATA
	@echo "Installing go-bindata"
	go get -u github.com/jteeuwen/go-bindata/...
endif
	@touch .godep

.jsdep: ui/yarn.lock
ifndef YARN
	$(error Please install yarn 0.19.1+)
else
	cd ui && yarn --no-progress --no-emoji
	@touch .jsdep
endif

gen: internal.pb.go

internal.pb.go: bolt/internal/internal.proto
	go generate -x ./bolt/internal

test: jstest gotest gotestrace

gotest:
	go test ./...

gotestrace:
	go test -race ./...

jstest:
	cd ui && yarn test

run: ${BINARY}
	./chronograf

run-dev: chronogiraffe
	./chronograf -d --log-level=debug

run-hmr:
	cd ui && npm run start:hmr

clean:
	if [ -f ${BINARY} ] ; then rm ${BINARY} ; fi
	cd ui && yarn run clean
	cd ui && rm -rf node_modules
	rm -f dist/dist_gen.go canned/bin_gen.go server/swagger_gen.go
	@rm -f .godep .jsdep .jssrc .dev-jssrc .bindata

ctags:
	ctags -R --languages="Go" --exclude=.git --exclude=ui .
