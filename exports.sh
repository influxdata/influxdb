#!/usr/bin/env bash

cd `dirname $0`

export GOPATH=`pwd`

if [ -d $HOME/go ]; then
    export GOROOT=$HOME/go
fi

pushd src
export packages=$(ls -d * | egrep -v 'google|launchpad|github' | tr '\n' ' ')
popd

