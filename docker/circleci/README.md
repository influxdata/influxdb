# CI Image
This Docker image is used to run all jobs in Circle CI. It includes all the test infrastructure needed
for unit and e2e tests, along with cross-compilers used to release InfluxDB for various architectures.

## Rebuilding the image
We haven't automated building the image on git-push (yet). For now, run the following from within this
directory whenever a change needs to be pushed:
```bash
VERSION=${VERSION:-latest} # Set some other version if you want
docker build -t quay.io/influxdb/influxdb-circleci:${VERSION} .
docker push quay.io/influxdb/influxdb-circleci:${VERSION}
```
