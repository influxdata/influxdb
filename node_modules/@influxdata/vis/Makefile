nodebin := ./node_modules/.bin
srcfiles := $(shell find src)

build: dist

run: node_modules
	rm -rf dist
	NODE_ENV=development $(nodebin)/rollup -cw

test: node_modules
	$(nodebin)/eslint 'src/**/*.{ts,tsx}' 'stories/**/*.{ts,tsx}'
	$(nodebin)/tsc --noEmit
	$(nodebin)/jest

clean:
	rm -rf dist node_modules .out .rpt*

publish: node_modules test build
	@git symbolic-ref HEAD | grep master  # Assert current branch is master
	$(nodebin)/bump --commit "Release v%s" --tag "v%s" --push
	npm publish --access=public

dist: node_modules $(srcfiles) tsconfig.json
	rm -rf dist
	NODE_ENV=production $(nodebin)/rollup -c

node_modules:
	npm install

run-docs: node_modules
	@npm run storybook  # Run via npm to support Chromatic

build-docs: node_modules
	$(nodebin)/build-storybook -o .out

publish-docs: node_modules
	PATH="$(PATH):$(nodebin)" $(nodebin)/storybook-to-ghpages --out=.out

test-chromatic:
	scripts/test-chromatic.sh

.PHONY: clean test build publish run run-docs build-docs publish-docs test-chromatic
