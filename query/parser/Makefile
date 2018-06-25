all: flux.go

flux.go: flux.peg parser.go parser_debug.go ../../bin/$(GOOS)/pigeon
	PATH=../../bin/${GOOS}:${PATH} $(GO_GENERATE) -x ./...

clean:
	rm -f flux.go

.PHONY: all clean
