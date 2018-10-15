targets := meta.pb.go

GO_GENERATE := go generate

all: $(targets)

$(targets): meta.proto
	$(GO_GENERATE) -x

clean:
	rm -f $(targets)

.PHONY: all clean
