#!/usr/bin/env bash

cd `dirname $0`

export GOPATH=`pwd`

if [ -d $HOME/go ]; then
    export GOROOT=$HOME/go
fi

pushd src
export packages=$(ls -d * | egrep -v 'google|launchpad|github' | tr '\n' ' ')
popd

snappy_dir=/tmp/snappychronosdb
leveldb_dir=/tmp/leveldbchronosdb
export LD_LIBRARY_PATH=/usr/local/lib
if [ `uname` == "Linux" ]; then
    export CGO_CFLAGS="-I$leveldb_dir/include"
    export CGO_LDFLAGS="$leveldb_dir/libleveldb.a $snappy_dir/.libs/libsnappy.a -lstdc++"
else
    export CGO_LDFLAGS="-lleveldb -lsnappy -lstdc++"
fi
