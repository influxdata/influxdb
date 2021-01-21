#!/usr/bin/env bash
#
# Produce a deploy JSON file for a docker image on the host.
#
# Usage:
# 	./get-deploy-tags DOCKER_TAG_NAME
#
# For example, if building a docker image with a tag set:
# 	docker build -t infuxdata.io/iox:awesome
# or manually tagged:
# 	docker tag infuxdata.io/iox:build infuxdata.io/iox:awesome:awesome
#
# And the image is on the host, executing this script with:
#	./get-deploy-tags "awesome"
#
# Will generate the deployment JSON file.

set -euo pipefail

DOCKER_IMAGE_TAG=${1}
DOCKER_IMAGE="quay.io/influxdb/fusion"
APP_NAME="IOx"

DOCKER_IMAGE_INFO="$(docker images "${DOCKER_IMAGE}" --format '{{.Tag}} {{.Digest}}' | grep "${DOCKER_IMAGE_TAG}" )"

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
	--arg appKey "$APP_NAME" \
	'$tagDigest | split(" ") as $td | {
		($appKey): {
		Tag: ($imgPrefix + ":" + $td[0]),
		Digest: ($imgPrefix + "@" + $td[1]),
		},
		PublishedAt: (now | todateiso8601)
	}'
