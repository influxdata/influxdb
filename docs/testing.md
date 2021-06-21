# Testing

This document covers details that are only relevant if you are developing IOx and running the tests.

## Object storage

### To run the tests or not run the tests

If you are testing integration with some or all of the object storage options, you'll have more
setup to do.

By default, `cargo test -p object_store` does not run any tests that actually contact
any cloud services: tests that do contact the services will silently pass.

To run integration tests, use `TEST_INTEGRATION=1 cargo test -p object_store`, which will run the
tests that contact the cloud services and fail them if the required environment variables aren't
set.

### Configuration differences when running the tests

When running `influxdb_iox run`, you can pick one object store to use. When running the tests,
you can run them against all the possible object stores. There's still only one
`INFLUXDB_IOX_BUCKET` variable, though, so that will set the bucket name for all configured object
stores. Use the same bucket name when setting up the different services.

Other than possibly configuring multiple object stores, configuring the tests to use the object
store services is the same as configuring the server to use an object store service. See the output
of `influxdb_iox run --help` for instructions.

## InfluxDB 2 Client

The `influxdb2_client` crate may be used by people using InfluxDB 2.0 OSS, and should be compatible
with both that and IOx. If you want to run the integration tests for the client against InfluxDB
2.0 OSS, you will need to set `TEST_INTEGRATION=1`.

If you have `docker` in your path, the integration tests for the `influxdb2_client` crate will run
integration tests against `influxd` running in a Docker container.

If you do not want to use Docker locally, but you do have `influxd` for InfluxDB
2.0 locally, you can use that instead by running the tests with the environment variable
`INFLUXDB_IOX_INTEGRATION_LOCAL=1`.

## Kafka Write Buffer

If you want to run integration tests with a Kafka instance serving as a write buffer, you will need
to set `TEST_INTEGRATION=1`.

You will also need to set `KAFKA_CONNECT` to the host and port where the tests can connect to a
running Kafka instance.

There is a Docker Compose file for running Kafka and Zookeeper using Docker in
`docker/ci-kafka-docker-compose.yml` that CI also uses to run the integration tests.

You have two options for running `cargo test`: on your local (host) machine (likely what you
normally do with tests), or within another Docker container (what CI does).

### Running `cargo test` on the host machine

If you want to compile the tests and run `cargo test` on your local machine, you can start Kafka
using the Docker Compose file with:

```
$ docker-compose -f docker/ci-kafka-docker-compose.yml up kafka
```

You can then run the tests with `KAFKA_CONNECT=localhost:9093`. To run just the Kafka integration
tests, the full command would then be:

```
TEST_INTEGRATION=1 KAFKA_CONNECT=localhost:9093 cargo test -p influxdb_iox --test end-to-end write_buffer
```

### Running `cargo test` in a Docker container

Alternatively, you can do what CI does by compiling the tests and running `cargo test` in a Docker
container as well. First, make sure you have the latest `rust:ci` image by running:

```
docker image pull quay.io/influxdb/rust:ci
```

Then run this Docker Compose command that uses `docker/Dockerfile.ci.integration`:

```
docker-compose -f docker/ci-kafka-docker-compose.yml up --build --force-recreate --exit-code-from rust
```

Because the `rust` service depends on the `kafka` service in the Docker Compose file, you don't
need to start the `kafka` service separately.
