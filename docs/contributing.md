Contributing to InfluxDB
========================

InfluxDB follows standard Go project structure. This means that all
your go development are done in $GOPATH/src. GOPATH can be any
directory under which InfluxDB and all it's dependencies will be
cloned. For more details on recommended go project's structure, see
[How to Write Go Code](http://golang.org/doc/code.html) and
[Go: Best Practices for Production Environments](http://peter.bourgon.org/go-in-production/), or you can just follow
the steps below.

Signing the CLA
---------------

If you are going to be contributing back to InfluxDB please take a
second to sign our CLA, which can be found
[on our website](http://influxdb.com/community/cla.html)

Installing go
-------------

We recommend using gvm, a Go version manager. For instructions
on how to install see
[the gvm page on github](https://github.com/moovweb/gvm). InfluxDB
currently works with Go 1.4 and up.

After installing gvm you can install and set the default go version by
running the following:

    gvm install go1.4
    gvm use go1.4 --default

Project structure
-----------------

First you need to setup the project structure:

    export GOPATH=$HOME/gocodez
    mkdir -p $GOPATH/src/github.com/influxdb
    cd $GOPATH/src/github.com/influxdb
    git clone git@github.com:influxdb/influxdb

You can add the line `export GOPATH=$HOME/gocodez` to your bash/zsh
file to be set for every shell instead of having to manually run it
everytime.

We have a pre commit hook to make sure code is formatted properly
and vetted before you commit any changes. We strongly recommend using the pre
commit hook to guard against accidentally committing unformatted
code. To use the pre-commit hook, run the following:

    cd $GOPATH/src/github.com/influxdb/influxdb
    cp .hooks/pre-commit .git/hooks/

In case the commit is rejected because it's not formatted you can run
the following to format the code:

```
go fmt ./...
go vet ./...
```

To install go vet, run the following command:
```
go get golang.org/x/tools/cmd/vet
```

For more information on `go vet`, [read the GoDoc](https://godoc.org/golang.org/x/tools/cmd/vet).

Build and Test
-----

Make sure you have Go installed and the project structure as shown above. To then build the project, execute the following commands:

```bash
cd $GOPATH/src/github.com/influxdb
go get -u -f ./...
go build ./...
```

Once compilation completes, the binaries can be found in `$GOPATH/bin`. Please note that the InfluxDB binary is named `influxd`, not `influxdb`.

To set the version and commit flags during the build pass the following to the build command:

```bash
-ldflags="-X main.version $VERSION -X main.commit $COMMIT"
```

where $VERSION is the version, and $COMMIT is the git commit hash.

To run the tests, execute the following command:

```bash
cd $GOPATH/src/github.com/influxdb/influxdb
go test -v ./...

# run tests that match some pattern
go test -run=TestDatabase . -v

# run tests and show coverage
go test -coverprofile /tmp/cover . && go tool cover -html /tmp/cover
```

Useful links
------------
- [Useful techniques in Go](http://arslan.io/ten-useful-techniques-in-go)
- [Go in production](http://peter.bourgon.org/go-in-production/)
- [Principles of designing Go APIs with channels](https://inconshreveable.com/07-08-2014/principles-of-designing-go-apis-with-channels/)
- [Common mistakes in Golang](http://soryy.com/blog/2014/common-mistakes-with-go-lang/). Especially this section `Loops, Closures, and Local Variables`
