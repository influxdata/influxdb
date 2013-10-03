#!/usr/bin/env bash

cd `dirname $BASH_SOURCE`

export GOPATH=`pwd`

if [ -d $HOME/go ]; then
    export GOROOT=$HOME/go
fi
