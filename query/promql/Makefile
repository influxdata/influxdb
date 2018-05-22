all: promql.go

promql.go: promql.peg gen.go ../../bin/pigeon
	PATH=../../bin:${PATH} go generate -x ./...

clean:
	rm -f promql.go

.PHONY: all clean
