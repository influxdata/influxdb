#!/usr/bin/env bash

set -e

. ./exports.sh

go get code.google.com/p/goprotobuf/proto
go get github.com/goraft/raft
go get github.com/gorilla/mux
go get github.com/jmhodges/levigo >/dev/null 2>&1 || echo "levigo build will probably fail since we don't have leveldb or snappy"
go get github.com/bmizerany/pat
go get github.com/fitstar/falcore
go get github.com/fitstar/falcore/filter
go get code.google.com/p/log4go
go get code.google.com/p/go.crypto/bcrypt
go get launchpad.net/gocheck
go get github.com/influxdb/go-cache

patch="off"
cflags="-m32"
arch=80386
if [ "x$GOARCH" != "x386" ]; then
    arch=x86-64
    cflags=
    patch=on
fi
echo "Building for architecutre $arch"

if file $snappy_dir/.libs/libsnappy.so* | grep $arch >/dev/null 2>&1; then
    architecture_match=yes
fi

# build snappy and leveldb
if [ $on_linux == yes ]; then
    snappy_version=1.1.0
    snappy_file=snappy-$snappy_version.tar.gz

    # path to leveldb and snappy patches
    snappy_patch=$GOPATH/leveldb-patches/0001-use-the-old-glibc-memcpy-snappy.patch
    leveldb_patch=$GOPATH/leveldb-patches/0001-use-the-old-glibc-memcpy-leveldb.patch

    if [ ! -d $snappy_dir -o ! -e $snappy_dir/$snappy_file -o ! -e $snappy_dir/.libs/libsnappy.a -o "x$architecture_match" != "xyes" ]; then
        rm -rf $snappy_dir
        mkdir -p $snappy_dir
        pushd $snappy_dir
        wget https://snappy.googlecode.com/files/$snappy_file
        tar --strip-components=1 -xvzf $snappy_file
        # apply the path to use the old memcpy and avoid any references to the GLIBC_2.14 only if building the x64
        [ $patch == on ] && (patch -p1 < $snappy_patch || (echo "Cannot patch this version of snappy" && exit 1))
        CFLAGS=$cflags CXXFLAGS=$cflags ./configure
        make
        popd
    fi

    leveldb_version=1.12.0
    leveldb_file=leveldb-$leveldb_version.tar.gz
    if [ ! -d $leveldb_dir -o ! -e $leveldb_dir/$leveldb_file -o ! -e $leveldb_dir/libleveldb.a -o "x$architecture_match" != "xyes" ]; then
        rm -rf $leveldb_dir
        mkdir -p $leveldb_dir
        pushd $leveldb_dir
        wget https://leveldb.googlecode.com/files/$leveldb_file
        tar --strip-components=1 -xvzf $leveldb_file
        # apply the path to use the old memcpy and avoid any references to the GLIBC_2.14 only if building the x64
        [ $patch == on ] && (patch -p1 < $leveldb_patch || (echo "Cannot patch this version of leveldb" && exit 1))
        CXXFLAGS="-I$snappy_dir $cflags" LDFLAGS="-L$snappy_dir/.libs" make
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

for i in $packages; do
    go build $i
done

# make sure that the server doesn't use a new version of glibc
if [ $on_linux == yes ]; then
    if readelf -a ./server | grep GLIBC_2.14 >/dev/null 2>&1; then
	echo "./server has some references to GLIBC_2.14. Aborting."
	exit 1
    fi
fi
