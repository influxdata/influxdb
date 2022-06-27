# InfluxDB IOx Client

This is the official Rust client library for connecting to InfluxDB IOx.

Currently only gRPC is supported.

## Using the gRPC Write Client

To write to IOx, create a connection and a write client, and then send line
protocol. Here is an example of creating an instance that connects to an IOx
server running at `http://127.0.0.1:8081` (the default bind address for the
gRPC endpoint of IOx when running in all-in-one mode) and sending a line of
line protocol:

```rust
#[tokio::main]
fn main() {
    use influxdb_iox_client::{
        write::Client,
        connection::Builder,
    };

    let mut connection = Builder::default()
        .build("http://127.0.0.1:8081")
        .await
        .unwrap();

    let mut client = Client::new(connection);

    // write a line of line protocol data
    client
        .write_lp("bananas", "cpu,region=west user=23.2 100",0)
        .await
        .expect("failed to write to IOx");
    }
}
```
