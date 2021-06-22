/// Test Protobuf/gRPC stubs. Public and not `#[cfg(test)]` because used in a (tested) doc example.
pub mod test_proto {
    tonic::include_proto!("influxdata.iox.test");
}
