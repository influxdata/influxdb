//! Entrypoint of delorean when used as a library (e.g. 'use delorean::...')
#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

use std::{error, fmt};

pub mod id;
pub mod line_parser;
pub mod storage;
pub mod time;
pub use delorean_generated_types as generated_types;

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

// Note: I would like to compile this module only in the 'test' cfg,
// but when I do so then other modules can not find them. For example:
//
// error[E0433]: failed to resolve: could not find `test` in `delorean`
//   --> src/server/write_buffer_routes.rs:353:19
//     |
// 353 |     use delorean::test::storage::TestDatabaseStore;
//     |                   ^^^^ could not find `test` in `delorean`
//
//#[cfg(test)]
pub mod test;
