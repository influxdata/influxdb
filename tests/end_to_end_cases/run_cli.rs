use std::{
    process::Command,
    time::{Duration, Instant},
};

use assert_cmd::prelude::CommandCargoExt;

use crate::common::server_fixture::BindAddresses;

#[test]
fn test_unreadable_store_early_exit() {
    // Check that the server process checks early for broken store configs and exits if the store is unusable. This
    // is important in our K8s setup since the proxy (istio for example) might come up late, so we don't wanna end
    // up with a global server error because of that.
    let addrs = BindAddresses::default();
    let mut process = Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .env("INFLUXDB_IOX_OBJECT_STORE", "s3")
        .env("AWS_ACCESS_KEY_ID", "foo")
        .env("AWS_SECRET_ACCESS_KEY", "bar")
        .env("INFLUXDB_IOX_BUCKET", "bucket")
        .env("INFLUXDB_IOX_BIND_ADDR", addrs.http_bind_addr())
        .env("INFLUXDB_IOX_GRPC_BIND_ADDR", addrs.grpc_bind_addr())
        .spawn()
        .unwrap();

    let t_start = Instant::now();
    loop {
        if t_start.elapsed() > Duration::from_secs(10) {
            process.kill().unwrap();
            panic!("Server process did not exit!");
        }

        if let Some(status) = process.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
    }
}
