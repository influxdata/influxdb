#!/bin/bash

# Bump this whenever a release branch is created from master
MASTER_VERSION=0.9.5

REPO_DIR=`mktemp -d`
echo "Using $REPO_DIR for all work..."

cd $REPO_DIR
export GOPATH=`pwd`
mkdir -p $GOPATH/src/github.com/influxdb
cd $GOPATH/src/github.com/influxdb
git clone https://github.com/influxdb/influxdb.git

cd $GOPATH/src/github.com/influxdb/influxdb
NIGHTLY_BUILD=true ./package.sh $MASTER_VERSION-nightly-`git log --pretty=format:'%h' -n 1`
rm -rf $REPO_DIR
