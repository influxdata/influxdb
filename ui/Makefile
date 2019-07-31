UISOURCES := $(shell find . -type f -not \( -path ./build/\* -o -path ./node_modules/\* -o -path ./.cache/\* -o -name Makefile -prune \) )

all: build

node_modules:
	yarn install

e2e: node_modules
	yarn test:junit

build: node_modules $(UISOURCES)
	yarn build

lint: node_modules $(UISOURCES)
	yarn lint

test:
	yarn test

clean:
	yarn clean

run:
	yarn start

.PHONY: all clean test run lint junit
