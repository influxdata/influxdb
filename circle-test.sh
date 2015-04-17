#!/bin/bash
#
# This is the InfluxDB CircleCI test script. Using this script allows total control
# the environment in which the build and test is run, and matches the official
# build process for InfluxDB.

GORACE="halt_on_error=1"
BUILD_DIR=$HOME/influxdb-build
GO_VERSION=go1.4

# Executes the given statement, and exits if the command returns a non-zero code.
function exit_if_fail {
    command=$@
    echo "Executing '$command'"
    $command
    rc=$?
    if [ $rc -ne 0 ]; then
        echo "'$command' returned $rc."
        exit $rc
    fi
}


source $HOME/.gvm/scripts/gvm
exit_if_fail gvm use $GO_VERSION
exit_if_fail go get golang.org/x/tools/cmd/vet

# Set up the build directory, and then GOPATH.
exit_if_fail mkdir $BUILD_DIR
export GOPATH=$BUILD_DIR
exit_if_fail mkdir -p $GOPATH/src/github.com/influxdb

# Move the checked-out source to a better location.
exit_if_fail mv $HOME/influxdb $GOPATH/src/github.com/influxdb
exit_if_fail cd $GOPATH/src/github.com/influxdb/influxdb
exit_if_fail git branch --set-upstream-to=origin/$CIRCLE_BRANCH $CIRCLE_BRANCH

# Install the code.
exit_if_fail cd $GOPATH/src/github.com/influxdb/influxdb
exit_if_fail go get -f -u -v -t ./...
exit_if_fail git checkout $CIRCLE_BRANCH # 'go get' switches to master. Who knew? Switch back.
exit_if_fail go build -v ./...

# Run the tests.
exit_if_fail go tool vet .
case $CIRCLE_NODE_INDEX in
    0)
        exit_if_fail go test -parallel 1000 -v -timeout 300s ./...
        ;;
    1)
        exit_if_fail go test -parallel 1000 -v -race -timeout 300s ./...
        ;;
esac

# Just to be nice.
exit 0
