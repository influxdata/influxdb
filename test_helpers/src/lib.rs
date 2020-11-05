#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use std::{env, f64, sync::Arc};
pub use tempfile;

pub mod tracing;

pub type Error = Box<dyn std::error::Error + Send + Sync + 'static>;
pub type Result<T = (), E = Error> = std::result::Result<T, E>;

/// A test helper function for asserting floating point numbers are within the machine epsilon
/// because strict comparison of floating point numbers is incorrect
pub fn approximately_equal(f1: f64, f2: f64) -> bool {
    (f1 - f2).abs() < f64::EPSILON
}

pub fn all_approximately_equal(f1: &[f64], f2: &[f64]) -> bool {
    f1.len() == f2.len() && f1.iter().zip(f2).all(|(&a, &b)| approximately_equal(a, b))
}

pub fn tmp_dir() -> Result<tempfile::TempDir> {
    let _ = dotenv::dotenv();

    let root = env::var_os("TEST_INFLUXDB_IOX_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

    Ok(tempfile::Builder::new()
        .prefix("influxdb_iox")
        .tempdir_in(root)?)
}

/// convert form that is easier to type in tests to what some code needs
pub fn str_vec_to_arc_vec(str_vec: &[&str]) -> Arc<Vec<Arc<String>>> {
    Arc::new(str_vec.iter().map(|s| Arc::new(String::from(*s))).collect())
}

/// convert form that is easier to type in tests to what some code needs
pub fn str_pair_vec_to_vec(str_vec: &[(&str, &str)]) -> Vec<(Arc<String>, Arc<String>)> {
    str_vec
        .iter()
        .map(|(s1, s2)| (Arc::new(String::from(*s1)), Arc::new(String::from(*s2))))
        .collect()
}

pub fn enable_logging() {
    std::env::set_var("RUST_LOG", "debug");
    env_logger::init();
}
