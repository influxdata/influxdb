## Builds

Builds are run from a docker build image that is configured with the node and go we support.
Our circle.yml uses this docker container to build, test and create release packages.

### Updating new node/go versions
After updating the Dockerfile_build run

`docker build -t quay.io/influxdb/builder:chronograf-$(date "+%Y%m%d") -f Dockerfile_build .`

and push to quay with:
`docker push quay.io/influxdb/builder:chronograf-$(date "+%Y%m%d")`

### Update circle
Update DOCKER_TAG in circle.yml to the new container.
