//! Definitions of catalog snapshots
//!
//! Snapshots are read-optimised, that is they are designed to be inexpensive to
//! decode, making extensive use of zero-copy [`Bytes`](bytes::Bytes) in place of
//! allocating structures such as `String` and `Vec`

pub mod hash;
pub mod list;
pub mod mask;
pub mod partition;
pub mod table;
