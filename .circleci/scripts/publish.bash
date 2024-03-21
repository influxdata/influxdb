#!/bin/bash
set -euo pipefail

release() {
  image_src="${1}:latest"
  image_dst="us-east1-docker.pkg.dev/influxdata-team-edge/influxdb3-edge/${1}:${2}"

  if docker pull "${image_dst}" ; then
    echo "docker image ${image_dst} already exists"
    exit 0
  fi
  docker tag  "${image_src}" "${image_dst}"
  docker push "${image_dst}"
}

release "${1}" "${CIRCLE_SHA1}"
if [[ "${CIRCLE_BRANCH}" == main ]] ; then
  release "${1}" latest
fi
