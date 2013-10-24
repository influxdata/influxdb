#!/usr/bin/env bash

cd `dirname $0`

export GOPATH=`pwd`

if [ -d $HOME/go ]; then
    export GOROOT=$HOME/go
fi

pushd src
all_packages=$(find . -type d | egrep -v 'google|launchpad|github' | tr '\n' ' ' | sed 's/\.\///g')
packages=""
for i in $all_packages; do
    if [ $i == "." ]; then
        continue
    fi
    if [ `ls $i/*.go 2>/dev/null | wc -l` -ne 0 ]; then
        packages="$packages $i"
    fi
done
popd

snappy_dir=/tmp/snappychronosdb
leveldb_dir=/tmp/leveldbchronosdb
export LD_LIBRARY_PATH=/usr/local/lib
if [ `uname` == "Linux" -a "x$TRAVIS" != "xtrue" ]; then
    export CGO_CFLAGS="-I$leveldb_dir/include"
    export CGO_LDFLAGS="$leveldb_dir/libleveldb.a $snappy_dir/.libs/libsnappy.a -lstdc++"
else
    export CGO_LDFLAGS="-lleveldb -lsnappy -lstdc++"
fi
