#!/bin/bash
set -x
docker_tag="chronograf-$(date +%Y%m%d)"

docker build --rm=false -f etc/Dockerfile_build -t builder:$docker_tag .
docker tag builder:$docker_tag quay.io/influxdb/builder:$docker_tag

docker push quay.io/influxdb/builder:$docker_tag
