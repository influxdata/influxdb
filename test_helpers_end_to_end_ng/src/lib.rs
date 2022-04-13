use rand::{distributions::Alphanumeric, thread_rng, Rng};

mod addrs;
mod client;
mod config;
mod database;
mod mini_cluster;
mod server_fixture;
mod server_type;
mod steps;

pub use client::*;
pub use config::TestConfig;
pub use mini_cluster::MiniCluster;
pub use server_fixture::{ServerFixture, TestServer};
pub use server_type::ServerType;
pub use steps::{Step, StepTest};

/// Return a random string suitable for use as a database name
pub fn rand_name() -> String {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(10)
        .map(char::from)
        .collect()
}

// Helper macro to skip tests if TEST_INTEGRATION and TEST_INFLUXDB_IOX_CATALOG_DSN environment
// variables are not set.
#[macro_export]
macro_rules! maybe_skip_integration {
    () => {{
        use std::env;
        dotenv::dotenv().ok();

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
            (false, Some(_)) => {
                eprintln!("skipping NG end-to-end integration tests - set TEST_INTEGRATION to run");
                return;
            }
            (false, None) => {
                eprintln!(
                    "skipping NG end-to-end integration tests - set TEST_INTEGRATION and \
                    TEST_INFLUXDB_IOX_CATALOG_DSN to run"
                );
                return;
            }
        }
    }};
}
