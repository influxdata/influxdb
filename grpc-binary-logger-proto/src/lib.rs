#![allow(clippy::derive_partial_eq_without_eq)]
pub mod proto {
    tonic::include_proto!("grpc.binarylog.v1");
}
pub use proto::*;
