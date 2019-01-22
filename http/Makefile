# List any generated files here
TARGETS = ../ui/src/api/api.ts
# List any source files used to generate the targets here
SOURCES = cur_swagger.yml
# List any directories that have their own Makefile here
SUBDIRS =

# Default target
all: $(SUBDIRS) $(TARGETS) swagger_gen.go

# Recurse into subdirs for same make goal
$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

# Clean all targets recursively
clean: $(SUBDIRS)
	rm -f $(TARGETS)
	rm -f swagger_gen.go

swagger_gen.go: swagger.go redoc.go swagger.yml
	go generate -x
	echo '//lint:file-ignore ST1005 Ignore error strings should not be capitalized' >> swagger_gen.go

GO_RUN := env GO111MODULE=on go run

ifndef SKIP_CLIENT
../ui/src/api/api.ts: $(SOURCES)
	openapi-generator generate -g typescript-axios -o ../ui/src/api -i cur_swagger.yml
endif

.PHONY: all clean $(SUBDIRS)
