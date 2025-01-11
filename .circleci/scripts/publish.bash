#!/bin/bash
set -euo pipefail

release() {
  # This is a raw name, e.g. influxdb3
  image_name="${1}"
  image_dst="quay.io/influxdb/${1}:${2}"


  # Tag images for each architecture
  docker tag  "${image_name}:latest-amd64" "${image_dst}-amd64"
  docker tag  "${image_name}:latest-arm64" "${image_dst}-arm64"

  # push images for each architecture
  docker push "${image_dst}-amd64"
  docker push "${image_dst}-arm64"

  docker manifest create "${image_dst}" \
    --amend "${image_dst}-amd64" \
    --amend "${image_dst}-arm64"

  # Annotate the manifest with architecture and OS information
  docker manifest annotate "${image_dst}" \
    "${image_dst}-amd64" --arch amd64 --os linux
  docker manifest annotate "${image_dst}" \
    "${image_dst}-arm64" --arch arm64 --os linux

  # Push up the manifest to create a multi-arch image.
  docker manifest push "${image_dst}"
}

release "${1}" "${CIRCLE_SHA1}"
if [[ "${CIRCLE_BRANCH}" == main ]] ; then
  release "${1}" latest
fi
