#![deny(rust_2018_idioms)]
#![warn(clippy::explicit_iter_loop)]

use std::{error, fmt};

pub mod encoders;
pub mod id;
pub mod line_parser;
pub mod storage;
pub mod time;

pub mod delorean {
    include!(concat!(env!("OUT_DIR"), "/influxdata.platform.storage.rs"));
}

// TODO: audit all errors and their handling in main

#[derive(Debug, Clone, PartialEq)]
pub struct Error {
    pub description: String,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        // Generic error, underlying cause isn't tracked.
        None
    }
}

#[cfg(test)]
pub mod tests {
    use std::f64;

    /// A test helper function for asserting floating point numbers are within the machine epsilon
    /// because strict comparison of floating point numbers is incorrect
    pub fn approximately_equal(f1: f64, f2: f64) -> bool {
        (f1 - f2).abs() < f64::EPSILON
    }
}
