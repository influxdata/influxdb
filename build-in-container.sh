#!/bin/bash

## tag
GIT_ORG_TAG=$(git describe --abbrev=0 --tags)
git describe --exact-match --abbrev=0 > /dev/null
if [ $? -ne 0 ];then
    GIT_TAG="${GIT_ORG_TAG}-dirty"
    BC_CMD=$(which bc)
    if [ $? -ne 0 ];then
        echo "!! Need bc command to calculate the number of commits -> proceed without..."
    else
        ## commit since tags
        CNT_ALL=$(git log --oneline |wc -l)
        CNT_COMMITS=$(echo "${CNT_ALL}-$(git log --oneline ${GIT_ORG_TAG} |wc -l)" |bc)
        GIT_TAG="${GIT_TAG}-${CNT_COMMITS}"
    fi
fi

## OS
if [ -f /etc/os-release ];then
    . /etc/os-release
    if [ "X${ID}" != "Xalpine" ];then
      ID=Linux
    fi
else
    ID=$(uname -s)
fi
WDIR=$(pwd)
if [ ! -d ${WDIR}/bin/ ];then
    echo "No target directory (${WDIR}/bin/)... "
    exit 1
fi

## CLI
cd ${WDIR}/cmd/influx
go get -d
echo "> go build -o ./bin/influx_${GIT_TAG}_${ID}"
go build -o ${WDIR}/bin/influx_${GIT_TAG}_${ID}

## Server
cd ${WDIR}/cmd/influxd
go get -d
echo "> go build -o ./bin/influxd_${GIT_TAG}_${ID}"
go build -o ${WDIR}/bin/influxd_${GIT_TAG}_${ID}
