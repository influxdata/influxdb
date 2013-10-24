#!/usr/bin/env bash

set -e

. ./exports.sh

go get code.google.com/p/goprotobuf/proto
go get github.com/goraft/raft
go get github.com/gorilla/mux
go get github.com/jmhodges/levigo || echo "levigo build will probably fail since we don't have leveldb or snappy"
go get github.com/bmizerany/pat
go get github.com/fitstar/falcore
go get github.com/fitstar/falcore/filter
go get code.google.com/p/log4go
go get code.google.com/p/go.crypto/bcrypt
go get launchpad.net/gocheck

# build snappy and leveldb
if [ `uname` == "Linux" -a "x$TRAVIS" != "xtrue" ]; then
    snappy_version=1.1.0
    snappy_file=snappy-$snappy_version.tar.gz
    if [ ! -d $snappy_dir -o ! -e $snappy_dir/$snappy_file -o ! -e $snappy_dir/.libs/libsnappy.a ]; then
        rm -rf $snappy_dir
        mkdir -p $snappy_dir
        pushd $snappy_dir
        wget https://snappy.googlecode.com/files/$snappy_file
        tar --strip-components=1 -xvzf $snappy_file
        ./configure
        make
        popd
    fi

    leveldb_version=1.12.0
    leveldb_file=leveldb-$leveldb_version.tar.gz
    if [ ! -d $leveldb_dir -o ! -e $leveldb_dir/$leveldb_file -o ! -e $leveldb_dir/libleveldb.a ]; then
        rm -rf $leveldb_dir
        mkdir -p $leveldb_dir
        pushd $leveldb_dir
        wget https://leveldb.googlecode.com/files/$leveldb_file
        tar --strip-components=1 -xvzf $leveldb_file
        CXXFLAGS="-I$snappy_dir" LDFLAGS="-L$snappy_dir/.libs" make
        popd
    fi

    pushd src/github.com/jmhodges/levigo/
    find . -name \*.go | xargs sed -i 's/\/\/ #cgo LDFLAGS: -lleveldb\|#cgo LDFLAGS: -lleveldb//g'
    popd
fi

if ! which protoc 2>/dev/null; then
    echo "Please install protobuf (protobuf-compiler on ubuntu) and try to run this script again"
    exit 1
fi

echo "packages: go build $packages"

./compile_protobuf.sh

pushd src/parser
./build_parser.sh
popd

go build $packages
