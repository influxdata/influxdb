#!/bin/bash
#
# This is the InfluxDB CircleCI test script. Using this script allows total control
# the environment in which the build and test is run, and matches the official
# build process for InfluxDB.

BUILD_DIR=$HOME/influxdb-build
GO_VERSION=go1.4.2
PARALLELISM="-parallel 256"
TIMEOUT="-timeout 480s"

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

# Check that go fmt has been run.
function check_go_fmt {
	fmtcount=`git ls-files | grep '.go$' | xargs gofmt -l 2>&1 | wc -l`
	if [ $fmtcount -gt 0 ]; then
	    echo "run 'go fmt ./...' to format your source code."
	    exit 1
	fi
}

# Check that go vet passes.
function check_go_vet {
	# Due to the way composites work, vet will fail for some of our tests so we ignore it
	vetcount=`go tool vet --composites=false ./ 2>&1  | wc -l`
	if [ $vetcount -gt 0 ]; then
	    echo "run 'go tool vet --composites=false ./' to see the errors it flags and correct your source code."
	    exit 1
	fi
}

source $HOME/.gvm/scripts/gvm
exit_if_fail gvm use $GO_VERSION

# Set up the build directory, and then GOPATH.
exit_if_fail mkdir $BUILD_DIR
export GOPATH=$BUILD_DIR
exit_if_fail mkdir -p $GOPATH/src/github.com/influxdb

# Dump some test config to the log.
echo "Test configuration"
echo "========================================"
echo "\$HOME: $HOME"
echo "\$GOPATH: $GOPATH"
echo "\$CIRCLE_BRANCH: $CIRCLE_BRANCH"

# Move the checked-out source to a better location.
exit_if_fail mv $HOME/influxdb $GOPATH/src/github.com/influxdb
exit_if_fail cd $GOPATH/src/github.com/influxdb/influxdb
exit_if_fail git branch --set-upstream-to=origin/$CIRCLE_BRANCH $CIRCLE_BRANCH

# Install the code.
exit_if_fail cd $GOPATH/src/github.com/influxdb/influxdb
exit_if_fail go get -t -d -v ./...
exit_if_fail git checkout $CIRCLE_BRANCH # 'go get' switches to master. Who knew? Switch back.
check_go_fmt
check_go_vet
exit_if_fail go build -v ./...

# Run the tests.
case $CIRCLE_NODE_INDEX in
    0)
        go test $PARALLELISM $TIMEOUT -v ./... 2>&1 | tee $CIRCLE_ARTIFACTS/test_logs.txt
        rc=${PIPESTATUS[0]}
        ;;
    1)
        INFLUXDB_DATA_ENGINE="tsm1" go test $PARALLELISM $TIMEOUT -v ./... 2>&1 | tee $CIRCLE_ARTIFACTS/test_logs.txt
        rc=${PIPESTATUS[0]}
        ;;
    2)
        # 32bit tests.
        if [[ -e ~/docker/image.tar ]]; then docker load -i ~/docker/image.tar; fi
        docker build -f Dockerfile_test_ubuntu32 -t ubuntu-32-influxdb-test .
        mkdir -p ~/docker; docker save ubuntu-32-influxdb-test > ~/docker/image.tar
        exit_if_fail docker build -f Dockerfile_test_ubuntu32 -t ubuntu-32-influxdb-test .
        docker run -v $(pwd):/root/go/src/github.com/influxdb/influxdb -e "CI=${CI}" \
                        -v ${CIRCLE_ARTIFACTS}:/tmp/artifacts \
                        -t ubuntu-32-influxdb-test bash \
                        -c "cd /root/go/src/github.com/influxdb/influxdb && go get -t -d -v ./... && go build -v ./... && go test ${PARALLELISM} ${TIMEOUT} -v ./... 2>&1 | tee /tmp/artifacts/test_logs_i386.txt && exit \${PIPESTATUS[0]}"
        rc=$?
        ;;
    3)
       GORACE="halt_on_error=1" go test $PARALLELISM $TIMEOUT -v -race ./... 2>&1 | tee $CIRCLE_ARTIFACTS/test_logs_race.txt
       rc=${PIPESTATUS[0]}
       ;;
esac

exit $rc
