#!/bin/bash

REPO_DIR=`mktemp -d`
echo "Using $REPO_DIR for all work..."

cd $REPO_DIR
export GOPATH=`pwd`
mkdir -p $GOPATH/src/github.com/influxdb
cd $GOPATH/src/github.com/influxdb
git clone https://github.com/influxdb/influxdb.git

cd $GOPATH/src/github.com/influxdb/influxdb
NIGHTLY_BUILD=true ./package.sh 0.9.4-nightly-`git log --pretty=format:'%h' -n 1`
rm -rf $REPO_DIR
