# List any generated files here
TARGETS = predicate.pb.go \
	storage_common.pb.go

# List any source files used to generate the targets here
SOURCES = gen.go \
	predicate.proto \
	storage_common.proto

# List any directories that have their own Makefile here
SUBDIRS =

# Default target
all: $(SUBDIRS) $(TARGETS)

# Recurse into subdirs for same make goal
$(SUBDIRS):
	$(MAKE) -C $@ $(MAKECMDGOALS)

# Clean all targets recursively
clean: $(SUBDIRS)
	rm -f $(TARGETS)

# Define go generate if not already defined
GO_GENERATE := go generate

$(TARGETS): $(SOURCES)
	$(GO_GENERATE) -x

.PHONY: all clean $(SUBDIRS)
