# Testing

This document covers details that are only relevant if you are developing IOx and running the tests.

## "End to End" tests

The purpose of the "end to end tests" is highest level "integration"
test that can be run entirely within the `influxdb_iox` repository
with minimum dependencies that ensure all the plumbing is connected
correctly.

It is NOT meant to cover corner cases in implementation, which are
better tested with targeted tests in the various sub modules that make
up IOx.

Each of these tests starts up IOx as a sub process (aka runs the
`influxdb_iox` binary) and manipulates it either via a client or the
CLI. These tests should *not* manipulate or use the contents of any
subsystem crate.

### Prerequisites
The end to end tests currently require a connection to postgres
specified by a `DSN`, such as
`postgresql://localhost:5432/alamb`. Note that the required schema is
created automatically.

### Running

The end to end tests are run using the `cargo test --test end_to_end` command, after setting the
`TEST_INTEGRATION` and `TEST_INFLUXDB_IOX_CATALOG_DSN` environment variables. NOTE if you don't set
these variables the tests will "pass" locally (really they will be skipped).

For example, to run the end to end tests assuming the example postgres DSN:

```shell
TEST_INTEGRATION=1 TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://localhost:5432/alamb cargo test --test end_to_end
```

You can also see more logging using the `LOG_FILTER` variable. For example:

```shell
LOG_FILTER=debug,sqlx=warn,h2=warn TEST_INTEGRATION=1  TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://localhost:5432/alamb cargo test --test end_to_end
```

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

When running `influxdb_iox run database`, you can pick one object store to use. When running the tests,
you can run them against all the possible object stores. There's still only one
`INFLUXDB_IOX_BUCKET` variable, though, so that will set the bucket name for all configured object
stores. Use the same bucket name when setting up the different services.

Other than possibly configuring multiple object stores, configuring the tests to use the object
store services is the same as configuring the server to use an object store service. See the output
of `influxdb_iox run database --help` for instructions.

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

By default, the integration tests for the Kafka-based write buffer are not run.

In order to run them you must set two environment variables:

* `TEST_INTEGRATION=1`
* `KAFKA_CONNECT` to a host and port where the tests can connect to a running Kafka broker

### Running Kafka Locally

[Redpanda](https://vectorized.io/redpanda/) is a Kafka-compatible broker that can be used to run the tests, and is used
by the CI to test IOx.

Either follow the instructions on the website to install redpanda directly onto your system, or alternatively
it can be run in a docker container with:

```
docker run -d --pull=always --name=redpanda-1 --rm \
    -p 9092:9092 \
    -p 9644:9644 \
    docker.vectorized.io/vectorized/redpanda:latest \
    redpanda start \
    --overprovisioned \
    --smp 1  \
    --memory 1G \
    --reserve-memory 0M \
    --node-id 0 \
    --check=false
```

It is then just a case of setting the environment variables and running the tests as normal

```
TEST_INTEGRATION=1 KAFKA_CONNECT=localhost:9093 cargo test
```

Or to just run the Kafka tests

```
TEST_INTEGRATION=1 KAFKA_CONNECT=localhost:9093 cargo test -p write_buffer kafka --nocapture
```
