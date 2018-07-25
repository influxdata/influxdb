#!/bin/bash
#
# Pass all CLI arguments to Chronograf builder Docker image (passing
# them to the build scripts)
#
# WARNING: This script passes your SSH and AWS credentials within the
# Docker image, so use with caution.
#

set -e

# Default SSH key to $HOME/.ssh/id_rsa if not set
test -z $SSH_KEY_PATH && SSH_KEY_PATH="$HOME/.ssh/id_rsa"
echo "Using SSH key located at: $SSH_KEY_PATH"

# Default docker tag if not specified
test -z "$DOCKER_TAG" && DOCKER_TAG="chronograf-20161121"

docker run \
       -e AWS_ACCESS_KEY_ID \
       -e AWS_SECRET_ACCESS_KEY \
       -v $SSH_KEY_PATH:/root/.ssh/id_rsa \
       -v ~/.ssh/known_hosts:/root/.ssh/known_hosts \
       -v $(pwd):/root/go/src/github.com/influxdata/platform/chronograf \
       quay.io/influxdb/builder:$DOCKER_TAG \
       "$@"
