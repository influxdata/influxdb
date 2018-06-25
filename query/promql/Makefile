all: promql.go

promql.go: promql.peg gen.go ../../bin/$(GOOS)/pigeon
	PATH=../../bin/${GOOS}:${PATH} go generate -x ./...

clean:
	rm -f promql.go

.PHONY: all clean
