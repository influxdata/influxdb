# List any generated files here
TARGETS = static_gen.go swagger_gen.go
# List any source files used to generate the targets here
SOURCES = data/build data/swagger.yml

# Default target
all: data/build $(TARGETS)

# Clean all targets recursively
clean:
	rm -f $(TARGETS)
	rm -r data

# Target for the built UI assets directory.
data/build:
	../scripts/fetch-ui-assets.sh

# Define go generate if not already defined
GO_GENERATE := go generate

# Run go generate for the targets
$(TARGETS): $(SOURCES)
	$(GO_GENERATE) -x

.PHONY: all clean
