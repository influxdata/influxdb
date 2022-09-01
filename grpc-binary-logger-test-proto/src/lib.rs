#![allow(clippy::derive_partial_eq_without_eq)]
pub mod proto {
    tonic::include_proto!("test");
}
pub use proto::*;
