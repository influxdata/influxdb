#!/usr/bin/env bash

. ./exports.sh

go get code.google.com/p/goprotobuf/proto
go get github.com/goraft/raft
go get github.com/gorilla/mux
go get github.com/jmhodges/levigo

# build snappy and leveldb
if [ `uname` == "Linux" ]; then
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
fi


pushd src/github.com/jmhodges/levigo/
find . -name \*.go | xargs sed -i 's/\/\/ #cgo LDFLAGS: -lleveldb\|#cgo LDFLAGS: -lleveldb//g'
popd

echo "packages: go build $packages"

go build $packages
