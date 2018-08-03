targets := meta.pb.go

all: $(targets)

$(targets): meta.proto
	PATH=../../../../../bin/${GOOS}:${PATH} $(GO_GENERATE) -x ./

clean:
	rm -f $(targets)

.PHONY: all clean
