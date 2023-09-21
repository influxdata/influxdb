#![warn(unused_crate_dependencies)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use rand::{
    distributions::{Alphanumeric, Standard},
    thread_rng, Rng,
};

mod addrs;
mod authz;
mod client;
mod config;
mod data_generator;
mod database;
mod error;
mod grpc;
mod mini_cluster;
mod server_fixture;
mod server_type;
pub mod snapshot_comparison;
mod steps;
mod udp_listener;

pub use addrs::BindAddresses;
pub use authz::Authorizer;
pub use client::*;
pub use config::TestConfig;
pub use data_generator::DataGenerator;
pub use error::{check_flight_error, check_tonic_status};
pub use grpc::GrpcRequestBuilder;
pub use mini_cluster::MiniCluster;
pub use server_fixture::{ServerFixture, TestServer};
pub use server_type::{AddAddrEnv, ServerType};
pub use steps::{FCustom, Step, StepTest, StepTestState};
pub use udp_listener::UdpCapture;

/// Return a random string suitable for use as a namespace name
pub fn rand_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

// return a random 16 digit string comprised of numbers suitable for
// use as a influxdb2 org_id or bucket_id
pub fn rand_id() -> String {
    thread_rng()
        .sample_iter(&Standard)
        .filter_map(|c: u8| {
            if c.is_ascii_digit() {
                Some(char::from(c))
            } else {
                // discard if out of range
                None
            }
        })
        .take(16)
        .collect()
}

/// Log the [`std::process::Command`] being run in a way that's convenient to copy-paste
fn log_command(command: &std::process::Command) {
    use observability_deps::tracing::info;

    let envs_for_printing: Vec<_> = command
        .get_envs()
        .map(|(key, value)| {
            format!(
                "{}={}",
                key.to_str().unwrap(),
                value.unwrap_or_default().to_str().unwrap()
            )
        })
        .collect();
    let envs_for_printing = envs_for_printing.join(" ");

    info!("Running command: `{envs_for_printing} {:?}`", command);
}

/// Dumps the content of the log file to stdout
fn dump_log_to_stdout(server_type: &str, log_path: &std::path::Path) {
    use observability_deps::tracing::info;
    use std::io::Read;

    let mut f = std::fs::File::open(log_path).expect("failed to open log file");
    let mut buffer = [0_u8; 8 * 1024];

    info!("****************");
    info!("Start {server_type} Output");
    info!("****************");

    while let Ok(read) = f.read(&mut buffer) {
        if read == 0 {
            break;
        }
        if let Ok(str) = std::str::from_utf8(&buffer[..read]) {
            print!("{str}");
        } else {
            info!(
                "\n\n-- ERROR IN TRANSFER -- please see {:?} for raw contents ---\n\n",
                log_path
            );
        }
    }

    info!("****************");
    info!("End {server_type} Output");
    info!("****************");
}

// Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment
// variables are not set.
#[macro_export]
macro_rules! maybe_skip_integration {
    ($panic_msg:expr) => {{
        use std::env;
        dotenvy::dotenv().ok();

        match (
            env::var("TEST_INTEGRATION").is_ok(),
            env::var("TEST_INFLUXDB_IOX_CATALOG_DSN").ok(),
        ) {
            (true, Some(dsn)) => dsn,
            (true, None) => {
                panic!(
                    "TEST_INTEGRATION is set which requires running integration tests, but \
                    TEST_INFLUXDB_IOX_CATALOG_DSN is not set. Please set \
                    TEST_INFLUXDB_IOX_CATALOG_DSN to the test catalog database. For example, \
                    `TEST_INFLUXDB_IOX_CATALOG_DSN=postgres://postgres@localhost/iox_shared_test` \
                    would connect to a Postgres catalog."
                )
            }
            (false, maybe_dsn) => {
                let unset_vars = match maybe_dsn {
                    Some(_) => "TEST_INTEGRATION",
                    None => "TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN",
                };

                eprintln!("skipping end-to-end integration tests - set {unset_vars} to run");

                let panic_msg: &'static str = $panic_msg;
                if !panic_msg.is_empty() {
                    panic!("{}", panic_msg);
                }

                return;
            }
        }
    }};
    () => {
        maybe_skip_integration!("")
    };
}
