all: ifql.go


ifql.go: ifql.peg parser.go parser_debug.go ../bin/pigeon
	PATH=../bin:${PATH} $(GO_GENERATE) -x ./...

clean:
	rm -f ifql.go

.PHONY: all clean
