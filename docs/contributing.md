Contributing to InfluxDB
========================

InfluxDB follows standard Go project structure. This means that all
your go development are done in $GOPATH/src. GOPATH can be any
directory under which InfluxDB and all it's dependencies will be
cloned. For more details on recommended go project's structure, see
the following great posts [http://golang.org/doc/code.html] and
[http://peter.bourgon.org/go-in-production/], or you can just follow
the steps below.

Signing the CLA
---------------

If you are going to be contributing back to InfluxDB please take a
second to sign our CLA, which can be found
[on our website](http://influxdb.com/community/cla.html)

Getting go
----------

I recommend using gvm which is a go version manager. For instructions
on how to install see
[gvm page on github](https://github.com/moovweb/gvm)

After installing gvm you can install and set the default go version by
running the following:

    gvm install go1.2.1
    gvm use go1.2.1 --default

Project structure
-----------------

First you need to setup the project structure

    export GOPATH=$HOME/gocodez
    mkdir -p $GOPATH/src/github.com/influxdb
    cd $GOPATH/src/github.com/influxdb
    git clone git@github.com:influxdb/influxdb

You can add the line `export GOPATH=$HOME/gocodez` to your bash/zsh
file to be set for every shell instead of having to manually run it
everytime.

We have a pre commit hook to make sure code is formatted properly
before you commit any changes. We strongly recommend using the pre
commit hook to guard against accidentally committing unformatted
code. To use the pre-commit hook, run the following:

    cd $GOPATH/src/github.com/influxdb/influxdb
    cp .hooks/pre-commit .git/hooks/

In case the commit is rejected because it's not formatted you can run
the following to format the code:

    make format

Build on OSX
------------

You'll need the following dependencies:

    brew install protobuf bison flex leveldb hg bzr

To build run the following command:

    ./configure \
      --with-flex=/usr/local/Cellar/flex/2.5.37/bin/flex \
      --with-bison=/usr/local/Cellar/bison/3.0.2/bin/bison && make

Build on Linux
--------------

You'll need the following dependencies:

    sudo apt-get install mercurial bzr protobuf-compiler flex bison \
      valgrind g++ make autoconf libtool libz-dev libbz2-dev curl \
      rpm build-essential git

or on Red Had-based distros:

    sudo yum install hg bzr protobuf-compiler flex bison valgrind g++ \
      make autoconf libtool zlib-dev bzip2-libs

To build run the following:

    ./configure && make

Building with RocksDB
---------------------

RocksDB requires GCC 4.7 to get C++11 support. See
[their installation instruction](https://github.com/facebook/rocksdb/blob/296e340753f23f213655ff1d4549c73fa0262038/INSTALL.md)
for more information. Currently our build system runs Centos 6.4 and
uses a compiled version of GCC 4.7

Common Make targets
-------------------

The following are make targets that can be used on any architecture:

- `build` Builds the binaries. This target generates `influxdb` binary
  in the root of the repo
- `test` Runs the unit tests in all packages. Can be run with
  `verbose=on` to increase verbosity and `only=<test-name>` to only
  run certain tests.
- `integration_test` Runs the integration test suite. Accepts tha same
  arguments as `test`. The integration tests are in the `integration`
  package.
- `clean` Cleans all dependencies and temporary files created by the Makefile.
- `format` Formats the entire codebase.

Useful links
------------
- [Useful techniques in Go](http://arslan.io/ten-useful-techniques-in-go)
- [Go in production](http://peter.bourgon.org/go-in-production/)
- [Principles of designing Go APIs with channels](https://inconshreveable.com/07-08-2014/principles-of-designing-go-apis-with-channels/)
- [Common mistakes in Golang](http://soryy.com/blog/2014/common-mistakes-with-go-lang/). Especially this section `Loops, Closures, and Local Variables`
