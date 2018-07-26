targets := tasks.pb.go

all: $(targets)

$(targets): tasks.proto
	PATH=../../../../bin/${GOOS}:${PATH} $(GO_GENERATE) -x ./

clean:
	rm -f $(targets)

.PHONY: all clean
