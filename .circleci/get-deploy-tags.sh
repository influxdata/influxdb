#!/usr/bin/env bash

# Accept the git commit SHA as the first arg, or try and parse it if not set.
DOCKER_IMAGE="quay.io/influxdb/fusion"
APPKEY="iox"

DOCKER_IMAGE_INFO="$(docker images "${DOCKER_IMAGE}" --format '{{.Tag}} {{.Digest}}')"

# validate that only one image with same git sha as tag is present, if not,
# write an error out and request developers to manually re-run the workflow
if [ "$(echo "${DOCKER_IMAGE_INFO}" | wc -l)" != "1" ] ; then
	echo >&2 "Unable to determine unique SHA256 checksum of images - available:"
	echo >&2 ""
	echo >&2 "${DOCKER_IMAGE_INFO}"
	echo >&2 ""
	echo >&2 "Please re-run the workflow in CircleCI"
	exit 1
fi
jq --null-input --sort-keys \
	--arg tagDigest "${DOCKER_IMAGE_INFO}" \
	--arg imgPrefix "${DOCKER_IMAGE}" \
	--arg appKey "$APPKEY" \
	'$tagDigest | split(" ") as $td | {
		($appKey): {
		Tag: ($imgPrefix + ":" + $td[0]),
		Digest: ($imgPrefix + "@" + $td[1]),
		}
	}'