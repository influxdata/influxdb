all: flux.go

flux.go: flux.peg parser.go parser_debug.go ../../bin/pigeon
	PATH=../../bin:${PATH} $(GO_GENERATE) -x ./...

clean:
	rm -f flux.go

.PHONY: all clean
