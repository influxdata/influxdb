all: cmpopts.go

cmpopts.go: ../ast.go gen.go ../../bin/cmpgen
	PATH=../../bin:${PATH} $(GO_GENERATE) -x ./...

clean:
	rm -f cmpopts.go

.PHONY: all clean
