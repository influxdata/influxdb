#![warn(unused_crate_dependencies)]
#![allow(
    clippy::derive_partial_eq_without_eq,
    clippy::future_not_send,
    missing_copy_implementations,
    unreachable_pub
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod proto {
    tonic::include_proto!("test");
}
pub use proto::*;
