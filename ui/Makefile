YARN := $(shell command -v yarn 2> /dev/null)
UISOURCES := $(shell find . -type f -not \( -path ./build/\* -o -path ./node_modules/\* -o -path ./.cache/\* -o -name Makefile -prune \) )

all: build

node_modules: yarn.lock
ifndef YARN
	$(error Please install yarn 0.19.1+)
else
	yarn --no-progress --emoji false
endif

build: node_modules $(UISOURCES)
ifndef YARN
	$(error Please install yarn 0.19.1+)
else
	yarn run build
endif

lint: node_modules $(UISOURCES)
ifndef YARN
	$(error Please install yarn 0.19.1+)
else
	yarn run tsc
endif


test:
ifndef YARN
	$(error Please install yarn 0.19.1+)
else
	yarn test --runInBand
endif

clean:
ifndef YARN
	$(error Please install yarn 0.19.1+)
else
	yarn run clean
endif

run:
	yarn run start

.PHONY: all clean test run lint
