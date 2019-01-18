SOURCES = $(shell find . -name '*.json')

all: bin_gen.go

clean:
	rm -f bin_gen.go

bin_gen.go: bin.go $(SOURCES)
	go generate -x

.PHONY: all clean
