UISOURCES := $(shell find . -type f -not \( -path ./build/\* -o -path ./node_modules/\* -o -path ./.cache/\* -o -name Makefile -prune \) )

all: build

node_modules:
	npm i

 e2e: node_modules
	yarn test:junit

build: node_modules $(UISOURCES)
	yarn build

lint: node_modules $(UISOURCES)
	yarn lint

test:
	npm test

clean:
	yarn clean

run:
	npm start

.PHONY: all clean test run lint junit
