use std::{
    io::Read,
    process::Command,
    time::{Duration, Instant},
};

use assert_cmd::prelude::CommandCargoExt;
use tempfile::NamedTempFile;

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
        .arg("database")
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

        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn test_deprecated_cli_without_server_type() {
    let addrs = BindAddresses::default();

    let (log_file, log_path) = NamedTempFile::new()
        .expect("opening log file")
        .keep()
        .expect("expected to keep");

    let stdout_log_file = log_file
        .try_clone()
        .expect("cloning file handle for stdout");

    let mut process = Command::cargo_bin("influxdb_iox")
        .unwrap()
        .arg("run")
        .env("INFLUXDB_IOX_BIND_ADDR", addrs.http_bind_addr())
        .env("INFLUXDB_IOX_GRPC_BIND_ADDR", addrs.grpc_bind_addr())
        .stdout(stdout_log_file)
        .spawn()
        .unwrap();

    let t_start = Instant::now();
    loop {
        if t_start.elapsed() > Duration::from_secs(10) {
            process.kill().unwrap();
            panic!("Cannot find expected output in time.");
        }

        let mut output = String::new();
        std::fs::File::open(&log_path)
            .unwrap()
            .read_to_string(&mut output)
            .unwrap();

        if output.contains("WARNING: Not specifying the run-mode is deprecated.") {
            break;
        }

        std::thread::sleep(Duration::from_millis(10));
    }

    // process is still running
    assert!(process.try_wait().unwrap().is_none());

    // kill process
    process.kill().unwrap();
    process.wait().unwrap();
}
