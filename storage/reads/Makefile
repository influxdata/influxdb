# List any generated files here
TARGETS = array_cursor.gen.go \
		response_writer.gen.go \
		stream_reader.gen.go \
		stream_reader_gen_test.go \
		table.gen.go

# List any source files used to generate the targets here
SOURCES = gen.go \
	array_cursor.gen.go.tmpl \
	array_cursor.gen.go.tmpldata \
	response_writer.gen.go.tmpl \
	stream_reader.gen.go.tmpl \
	stream_reader_gen_test.go.tmpl \
	table.gen.go.tmpl \
	types.tmpldata

# List any directories that have their own Makefile here
SUBDIRS = datatypes

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

# Run go generate for the targets
$(TARGETS): $(SOURCES)
	$(GO_GENERATE) -x

.PHONY: all clean $(SUBDIRS)
