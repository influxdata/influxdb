#![deny(rust_2018_idioms)]
#![allow(elided_lifetimes_in_paths)] // https://github.com/rust-lang/rust/issues/71957
#![warn(clippy::explicit_iter_loop)]

use std::{error, fmt};

pub mod encoders;
pub mod id;
pub mod line_parser;
pub mod storage;
pub mod time;

pub mod delorean {
    // The generated code doesn't conform to these lints
    #![allow(
        unused_imports,
        rust_2018_idioms,
        clippy::redundant_static_lifetimes,
        clippy::redundant_closure
    )]

    include!(concat!(env!("OUT_DIR"), "/influxdata.platform.storage.rs"));
    include!(concat!(env!("OUT_DIR"), "/wal_generated.rs"));

    // Can't implement `Default` because `prost::Message` implements `Default`
    impl TimestampRange {
        pub fn max() -> Self {
            TimestampRange {
                start: std::i64::MIN,
                end: std::i64::MAX,
            }
        }
    }
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
