# List any generated files here
TARGETS = static_gen.go
# List any source files used to generate the targets here
SOURCES = data/build data/swagger.json

# Default target
all: $(TARGETS) data/build data/swagger.json

# Clean all targets recursively
clean:
	rm -f $(TARGETS)
	rm -r data

# Target for the built UI assets directory.
data/build:
	../scripts/fetch-ui-assets.sh

# Target for the swagger.json from the openapi repo
data/swagger.json:
	OPENAPI_SHA=c87a08f832e79bc338a37509028641cda9e1875b ../scripts/fetch-swagger.sh

# Define go generate if not already defined
GO_GENERATE := go generate

# Run go generate for the targets
$(TARGETS): $(SOURCES)
	$(GO_GENERATE) -x

.PHONY: all clean
