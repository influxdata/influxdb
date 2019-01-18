SOURCES = $(shell find . -name '*.json')

all: bin_gen.go

clean:
	rm -f bin_gen.go

bin_gen.go: bin.go $(SOURCES)
	go generate -x
	echo '//lint:file-ignore ST1005 Ignore error strings should not be capitalized' >> bin_gen.go

.PHONY: all clean
