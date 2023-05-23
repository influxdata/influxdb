#![warn(unused_crate_dependencies)]
#![allow(clippy::derive_partial_eq_without_eq)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod proto {
    tonic::include_proto!("test");
}
pub use proto::*;
