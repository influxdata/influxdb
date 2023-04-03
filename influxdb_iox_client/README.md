# InfluxDB IOx Client

This is the Rust client library for connecting to InfluxDB IOx.

We're attempting to support all apis as they are added and modified but this client
is likely not 100% complete at any time.

Some apis are http (for instance the `write`) and some are gRPC. See the individual
client modules for details.

## Example: Using the Write Client

To write to IOx, create a connection and a write client, and then send line
protocol. Please see the example on ['Client' struct](./src/client/write.rs) that will work
when running against all-in-one mode.
