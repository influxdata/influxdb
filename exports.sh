#!/usr/bin/env bash

cd `dirname $0`

export GOPATH=`pwd`

if [ "x$GOROOT" = 'x' -a -d $HOME/go ]; then
    export GOROOT=$HOME/go
fi

pushd src
all_packages=$(find . -type d | egrep -v 'google|launchpad|github' | tr '\n' ' ' | sed 's/\.\///g')
packages=""
for i in $all_packages; do
    if [ $i = "." ]; then
        continue
    fi
    if [ `ls $i/*.go 2>/dev/null | wc -l` -ne 0 ]; then
        packages="$packages $i"
    fi
done
popd

snappy_dir=/tmp/snappy.influxdb.amd64
leveldb_dir=/tmp/leveldb.influxdb.amd64
export LD_LIBRARY_PATH=/usr/local/lib
on_linux="no"
if [ `uname` = "Linux" ]; then
    on_linux=yes

elif [ "x$CC" == "x" -a `uname -v | cut -d' ' -f4` = "13.0.0:" ]; then
    # for mavericks use gcc instead of llvm
    export CC=gcc-4.2
fi

if [ "x$PYTHONPATH" = x -a $on_linux != yes ]; then
    PYTHONPATH=/usr/local/lib/python2.7/site-packages/:$PYTHONPATH
fi

if [ $on_linux = yes ]; then
    export CGO_CFLAGS="-I$leveldb_dir/include"
    export CGO_LDFLAGS="$leveldb_dir/libleveldb.a $snappy_dir/.libs/libsnappy.a -lstdc++"
else
    export CGO_LDFLAGS="-lleveldb -lsnappy -lstdc++"
fi
