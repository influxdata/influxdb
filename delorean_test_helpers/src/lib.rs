#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations, clippy::explicit_iter_loop)]

use std::{env, f64};
use tempfile::TempDir;

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

pub fn tmp_dir() -> Result<TempDir> {
    let _ = dotenv::dotenv();

    let root = env::var_os("TEST_DELOREAN_DB_DIR").unwrap_or_else(|| env::temp_dir().into());

    Ok(tempfile::Builder::new()
        .prefix("delorean")
        .tempdir_in(root)?)
}
