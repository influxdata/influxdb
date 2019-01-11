#!/bin/bash
#
# Pull the required build image from quay.io.
#

if [[ -z "$DOCKER_TAG" ]]; then
    echo "Please specify a tag to pull from with the DOCKER_TAG env variable."
    exit 1
fi

docker pull quay.io/influxdb/builder:$DOCKER_TAG
