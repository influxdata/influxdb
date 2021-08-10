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
# Will generate the deployment JSON file and output it to stdout. If
# IMAGE_PROMOTION_COMMAND is set in the environment, the deployment JSON file is
# piped to it at the end of execution.

set -euo pipefail

DOCKER_IMAGE_TAG=${1}
DOCKER_IMAGE="quay.io/influxdb/fusion"
APP_NAME="IOx"

DOCKER_IMAGE_DIGEST="$(docker image inspect "${DOCKER_IMAGE}:${DOCKER_IMAGE_TAG}" --format '{{ if eq (len .RepoDigests) 1 }}{{index .RepoDigests 0}}{{ end }}')"

# validate that only one image with same git sha as tag is present, if not,
# write an error out and request developers to manually re-run the workflow
if [ "${DOCKER_IMAGE_DIGEST}" = "" ] ; then
	echo >&2 "Unable to determine unique SHA256 checksum of images."
	echo >&2 ""
	echo >&2 "Please re-run the workflow in CircleCI."
	exit 1
fi
jq --null-input --sort-keys \
	--arg imgTag "${DOCKER_IMAGE}:${DOCKER_IMAGE_TAG}" \
	--arg imgDigest "${DOCKER_IMAGE_DIGEST}" \
	--arg appKey "$APP_NAME" \
	'{
		Images: {
			($appKey): {
				Tag: $imgTag,
				Digest: $imgDigest,
			},
		},
		PublishedAt: (now | todateiso8601)
	}' | tee deploy.json

echo ""
if [[ -z "${IMAGE_PROMOTION_COMMAND}" ]]; then
	echo "Skipping image promotion (IMAGE_PROMOTION_COMMAND not set)"
else
	echo "Triggering image promotion"
	eval "${IMAGE_PROMOTION_COMMAND}" < deploy.json
	eval "${IMAGE_PROMOTION_COMMAND_K8S_IOX}" < deploy.json
fi
