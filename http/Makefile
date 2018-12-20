# List any generated files here
TARGETS = ../ui/src/api/api.ts
# List any source files used to generate the targets here
SOURCES = cur_swagger.yml
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

../ui/src/api/api.ts: $(SOURCES)
	cat cur_swagger.yml | go run ../internal/yaml2json > openapi.json
	openapi-generator generate -g typescript-axios -o ../ui/src/api -i openapi.json
	rm openapi.json

.PHONY: all clean $(SUBDIRS)
