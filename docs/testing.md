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

### Running

The end to end tests are run using the `cargo test --test end_to_end` command, after setting the
`TEST_INTEGRATION` and `TEST_INFLUXDB_IOX_CATALOG_DSN` environment variables. NOTE if you don't set
these variables the tests will "pass" locally (really they will be skipped).

By default, the integration tests for the Kafka-based write buffer are not run. To run these
you need to set the `KAFKA_CONNECT` environment variable and `TEST_INTEGRATION=1`.

For example, you can run this docker compose to get redpanda (a kafka-compatible message queue)
and postgres running:

```shell
docker-compose -f integration-docker-compose.yml up
```

In another terminal window, you can run:

```shell
export TEST_INTEGRATION=1
export KAFKA_CONNECT=localhost:9092
export TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres
cargo test --workspace
```

Or for just the end-to-end tests (and not general tests or kafka):

```shell
TEST_INTEGRATION=1 TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres cargo test --test end_to_end
```

If you are debugging a failing end-to-end test, you will likely want to run with `--nocapture` to also get the logs from the test execution in addition to the server:

```
cargo test --test end_to_end -- my_failing_test --nocapture
```

If running multiple tests in parallel:

* The output may be interleaved
* Multiple tests may share the same server instance and thus the server logs may be captured in the output of a different test than the one that is failing.

When debugging a failing test it is therefore recommended you run a single test, or disable parallel test execution

```
cargo test --test end_to_end -- --test-threads 1
```

You can also see more logging using the `LOG_FILTER` variable. For example:

```shell
LOG_FILTER=debug,sqlx=warn,h2=warn
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

When running `influxdb_iox run`, you can pick one object store to use. When running the tests, you
can run them against all the possible object stores. There's still only one `INFLUXDB_IOX_BUCKET`
variable, though, so that will set the bucket name for all configured object stores. Use the same
bucket name when setting up the different services.

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
