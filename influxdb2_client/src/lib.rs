#![deny(rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::use_self
)]

//! # influxdb2_client
//!
//! This is a Rust client to InfluxDB using the 2.0 API.
//!
//! ## Work Remaining
//!
//! - Write
//! - Query
//! - Authentication
//! - optional sync client
//! - Infux 1.x API?
//! - Other parts of the API

/// Client to a server supporting the InfluxData 2.0 API.
#[derive(Debug, Copy, Clone)]
pub struct Client {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let _client = Client {};
    }
}
