all: cmpopts.go

cmpopts.go: ../ast.go gen.go ../../../bin/$(GOOS)/cmpgen
	PATH=../../../bin/${GOOS}:${PATH} $(GO_GENERATE) -x ./...

clean:
	rm -f cmpopts.go

.PHONY: all clean
