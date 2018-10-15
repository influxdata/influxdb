all: promql.go

GO_GENERATE := go generate

promql.go: promql.peg gen.go
	$(GO_GENERATE) -x

clean:
	rm -f promql.go

.PHONY: all clean
