# Testing

This document covers details that are only relevant if you are developing IOx and running the tests.

## Object storage

### To run the tests or not run the tests

If you are testing integration with some or all of the object storage options, you'll have more
setup to do.

By default, `cargo test -p object_store` does not run any tests that actually contact
any cloud services: tests that do contact the services will silently pass.

To ensure you've configured object storage integration testing correctly, you can run
`TEST_INTEGRATION=1 cargo test -p object_store`, which will run the tests that contact the cloud
services and fail them if the required environment variables aren't set.

If you don't specify the `TEST_INTEGRATION` environment variable but you do configure some or all
of the object stores, the relevant tests will run.

### Configuration differences when running the tests

When running `influxdb_iox server`, you can pick one object store to use. When running the tests,
you can run them against all the possible object stores. There's still only one
`INFLUXDB_IOX_BUCKET` variable, though, so that will set the bucket name for all configured object
stores. Use the same bucket name when setting up the different services.

Other than possibly configuring multiple object stores, configuring the tests to use the object
store services is the same as configuring the server to use an object store service. See the output
of `influxdb_iox server --help` for instructions.

## InfluxDB IOx Client

The `influxdb_iox_client` crate might be used by people who are using a managed IOx server. In
other words, they might only use the `influxdb_iox_client` crate and not the rest of the crates in
this workspace. The tests in `influxdb_iox_client` see an IOx server in the same way as IOx servers
see the object store services: sometimes you'll want to run the tests against an actual server, and
sometimes you won't.

Like in the `object_store` crate, the `influxdb_iox_client` crate's tests use the
`TEST_INTEGRATION` environment variable to enforce running tests that use an actual IOx server.
Running `cargo test -p influxdb_iox_client` will silently pass tests that contact a server.

Start an IOx server in one terminal and run `TEST_INTEGRATION=1
TEST_IOX_ENDPOINT=http://127.0.0.1:8080 cargo test -p influxdb_iox_client` in another (where
`http://127.0.0.1:8080` is the address to the IOx HTTP server) to run the client tests against the
server. If you set `TEST_INTEGRATION` but not `TEST_IOX_ENDPOINT`, the integration tests will fail
because of the missed configuration. If you set `TEST_IOX_ENDPOINT` but not `TEST_INTEGRATION`, the
integration tests will be run.
