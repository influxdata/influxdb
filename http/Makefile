# List any directories that have their own Makefile here
SUBDIRS =

# Default target
all: $(SUBDIRS) swagger_gen.go

# Recurse into subdirs for same make goal
$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

# Clean all targets recursively
clean: $(SUBDIRS)
	rm -f swagger_gen.go

swagger_gen.go: swagger.go redoc.go swagger.yml
	go generate -x
	echo '//lint:file-ignore ST1005 Ignore error strings should not be capitalized' >> swagger_gen.go

GO_RUN := env GO111MODULE=on go run

.PHONY: all clean $(SUBDIRS)
