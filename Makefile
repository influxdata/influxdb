PACKAGES=$(shell find . -name '*.go' -print0 | xargs -0 -n1 dirname | sort --unique)

default:

metalint: deadcode cyclo aligncheck defercheck structcheck lint errcheck

deadcode:
	@deadcode $(PACKAGES) 2>&1

cyclo:
	@gocyclo -over 10 $(PACKAGES)

aligncheck:
	@aligncheck $(PACKAGES)

defercheck:
	@defercheck $(PACKAGES)


structcheck:
	@structcheck $(PACKAGES)

lint:
	@for pkg in $(PACKAGES); do golint $$pkg; done

errcheck:
	@for pkg in $(PACKAGES); do \
	  errcheck -ignorepkg=bytes,fmt -ignore=":(Rollback|Close)" $$pkg \
	done

tools:
	go get github.com/remyoudompheng/go-misc/deadcode
	go get github.com/alecthomas/gocyclo
	go get github.com/opennota/check/...
	go get github.com/golang/lint/golint
	go get github.com/kisielk/errcheck
	go get github.com/sparrc/gdm

linux:
	docker run --rm -ti -v $(CURDIR):/usr/local/src/github.com/influxdata/influxdb/ --workdir /usr/local/src/github.com/influxdata/influxdb/ qnib/golang ./build-qnib.sh
alpine:
	docker run --rm -ti -v $(CURDIR):/usr/local/src/github.com/influxdata/influxdb/ --workdir /usr/local/src/github.com/influxdata/influxdb/ qnib/alpn-go-dev ./build-qnib.sh

.PHONY: default,metalint,deadcode,cyclo,aligncheck,defercheck,structcheck,lint,errcheck,tools
