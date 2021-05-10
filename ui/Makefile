UISOURCES := $(shell find . -type f -not \( -path ./build/\* -o -path ./node_modules/\* -o -path ./.cache/\* -o -name Makefile -prune \) )

all: build

client:
	yarn generate

node_modules:
	yarn install

e2e: node_modules
	yarn generate
	yarn test:e2e

build:
	../scripts/fetch-ui-assets.sh

lint: node_modules $(UISOURCES)
	yarn generate
	yarn lint

test:
	yarn install
	yarn generate
	yarn test

clean:
	yarn clean

run:
	yarn start

.PHONY: all clean test run lint junit client
