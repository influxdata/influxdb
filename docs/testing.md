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

For example, you can run this docker compose to get postgres running:

```shell
docker-compose -f integration-docker-compose.yml up
```

In another terminal window, you can run:

```shell
export TEST_INTEGRATION=1
export TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres
cargo test --workspace
```

Or for just the end-to-end tests (and not general tests):

```shell
TEST_INTEGRATION=1 TEST_INFLUXDB_IOX_CATALOG_DSN=postgresql://postgres@localhost:5432/postgres cargo test --test end_to_end
```

#### Running without Postgres

It is possible to run the end to end tests without postgres by using
the sqlite file based catalog. To do so pass `sqlite` as the catalog dsn:

```shell
TEST_INTEGRATION=1 TEST_INFLUXDB_IOX_CATALOG_DSN=sqlite cargo test --test end_to_end
```

NOTE that this not fully supported and will sometimes generate errors
related to "namespace not found" or "file locked". See
https://github.com/influxdata/influxdb_iox/issues/7709 for more
details (and hopefully to help fix it!)

#### Debugging Hints

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

## InfluxDB 2 Client

The `influxdb2_client` crate may be used by people using InfluxDB 2.0 OSS, and should be compatible
with both that and IOx. If you want to run the integration tests for the client against InfluxDB
2.0 OSS, you will need to set `TEST_INTEGRATION=1`.

If you have `docker` in your path, the integration tests for the `influxdb2_client` crate will run
integration tests against `influxd` running in a Docker container.

If you do not want to use Docker locally, but you do have `influxd` for InfluxDB
2.0 locally, you can use that instead by running the tests with the environment variable
`INFLUXDB_IOX_INTEGRATION_LOCAL=1`.
