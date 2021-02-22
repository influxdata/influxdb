# InfluxDB IOx Client

This is the official Rust client library for connecting to InfluxDB IOx.

## Using the HTTP API

If you only want to use the HTTP API, create an `influxdb_iox_client::Client`. Here is an example of
creating an instance that connects to an IOx server running at `http://127.0.0.1:8080` and creating
a database named `telemetry`:

```rust
#[tokio::main]
fn main() {
    use data_types::database_rules::DatabaseRules;
    use influxdb_iox_client::ClientBuilder;

    let client = ClientBuilder::default()
        .build("http://127.0.0.1:8080")
        .expect("client should be valid");

    client
        .create_database("telemetry", &DatabaseRules::default())
        .await
        .expect("failed to create database");
}
```

## Using the Arrow Flight API

IOx supports an [Arrow Flight gRPC API](https://arrow.apache.org/docs/format/Flight.html). To use
it, enable the `flight` feature, which is off by default:

```toml
[dependencies]
influxdb_iox_client = { version = "[..]", features = ["flight"] }
```

Then you can create an `influxdb_iox_client::FlightClient` by using the `FlightClientBuilder`. Here
is an example of creating an instance that connects to an IOx server with its gRPC services
available at `http://localhost:8082` and using it to perform a query:

```rust
#[tokio::main]
fn main() {
    use data_types::database_rules::DatabaseRules;
    use influxdb_iox_client::FlightClientBuilder;

    let client = FlightClientBuilder::default()
        .build("http://127.0.0.1:8082")
        .expect("client should be valid");

    let mut query_results = client
        .perform_query(scenario.database_name(), sql_query)
        .await;

    let mut batches = vec![];

    while let Some(data) = query_results.next().await {
        batches.push(data);
    }
}
```
