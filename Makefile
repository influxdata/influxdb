.PHONY: assets dep clean test gotest gotestrace jstest run run-dev run-hmr ctags

VERSION ?= $(shell git describe --always --tags)
COMMIT ?= $(shell git rev-parse --short=8 HEAD)
GOBINDATA := $(shell go list -f {{.Root}}  github.com/jteeuwen/go-bindata 2> /dev/null)
YARN := $(shell command -v yarn 2> /dev/null)

SOURCES := $(shell find . -name '*.go' ! -name '*_gen.go' -not -path "./vendor/*" )
UISOURCES := $(shell find ui -type f -not \( -path ui/build/\* -o -path ui/node_modules/\* -prune \) )

LDFLAGS=-ldflags "-s -X main.version=${VERSION} -X main.commit=${COMMIT}"
BINARY=chronograf

.DEFAULT_GOAL := all

all: dep build

build: assets ${BINARY}

dev: dep dev-assets ${BINARY}

${BINARY}: $(SOURCES) .bindata .jsdep .godep
	go build -o ${BINARY} ${LDFLAGS} ./cmd/chronograf/main.go

define CHRONOGIRAFFE
             tLf          iCf.
            .CCC.         tCC:
             CGG;         CGG:
tG0Gt:       GGGGGGGGGGGGGGGG1        .,:,
LG1,,:1CC: .GGL;iLC1iii1LCi;GG1  .1GCL1iGG1
 LG1:::;i1CGGt;;;;;;L0t;;;;;;GGGC1;;::,iGC
   ,ii:. 1GG1iiii;;tfiC;;;;;;;GGCfCGCGGC,
        fGCiiiiGi1Lt;;iCLL,i;;;CGt
       fGG11iiii1C1iiiiiGt1;;;;;CGf
       .GGLLL1i1CitfiiL1iCi;;iLCGGt
        .CGL11LGCCCCCCCLLCGG1;1GG;
          CGL1tf1111iiiiiiL1ifGG,
           LGCff1fCt1tCfiiCiCGC
            LGGf111111111iCGGt
             fGGGGGGGGGGGGGGi
              ifii111111itL
              ;f1i11111iitf
              ;f1iiiiiii1tf
              :fi111iii11tf
              :fi111ii1i1tf
              :f111111ii1tt
              ,L111111ii1tt
              .Li1111i1111CCCCCCCCCCCCCCLt;
               L111ii11111ittttt1tttttittti1fC;
               f1111ii111i1ttttt1;iii1ittt1ttttCt.
               tt11ii111tti1ttt1tt1;11;;;;iitttifCCCL,
               11i1i11ttttti;1t1;;;ttt1;;ii;itti;L,;CCL
               ;f;;;;1tttti;;ttti;;;;;;;;;;;1tt1ifi .CCi
               ,L;itti;;;it;;;;;tt1;;;t1;;;;;;ii;t; :CC,
                L;;;;iti;;;;;;;;;;;;;;;;;;;;;;;i;L, ;CC.
                ti;;;iLLfffi;;;;;ittt11i;;;;;;;;;L   tCCfff;
                it;;;;;;L,ti;;;;;1Ltttft1t;;;;;;1t    ;CCCL;
                :f;;;;;;L.ti;;;;;tftttf1,f;;;;;;f:    ;CC1:
                .L;;;;;;L.t1;;;;;tt111fi,f;;;;;;L.
                 1Li;;iL1 :Ci;;;tL1i1fC, Lt;;;;Li
                  .;tt;     ifLt:;fLf;    ;LCCt,
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
	go test `go list ./... | grep -v /vendor/`

gotestrace:
	go test -race `go list ./... | grep -v /vendor/`

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
