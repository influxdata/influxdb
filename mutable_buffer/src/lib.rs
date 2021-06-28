//! Contains an in memory mutable buffer that stores incoming data in
//! a structure that is designed to be quickly appended to as well as queried
//!
//! The mutable buffer is structured in this way:
//!
//! ```text
//! ┌───────────────────────────────────────────────┐
//! │                                               │
//! │    ┌────────────────┐                         │
//! │    │    Database    │                         │
//! │    └────────────────┘                         │
//! │             │ one partition per               │
//! │             │ partition_key                   │
//! │             ▼                                 │
//! │    ┌────────────────┐                         │
//! │    │   Partition    │                         │
//! │    └────────────────┘                         │
//! │             │  one open Chunk                 │
//! │             │  zero or more closed            │
//! │             ▼  Chunks                         │
//! │    ┌────────────────┐                         │
//! │    │     Chunk      │                         │
//! │    └────────────────┘                         │
//! │             │  multiple Tables (measurements) │
//! │             ▼                                 │
//! │    ┌────────────────┐                         │
//! │    │     Table      │                         │
//! │    └────────────────┘                         │
//! │             │  multiple Columns               │
//! │             ▼                                 │
//! │    ┌────────────────┐                         │
//! │    │     Column     │                         │
//! │    └────────────────┘                         │
//! │                              MutableBuffer    │
//! │                                               │
//! └───────────────────────────────────────────────┘
//! ```
//!
//! Each row of data is routed into a particular partitions based on
//! column values in that row. The partition's open chunk
//! is updated with the new data.
//!
//! The currently open chunk in a partition can be rolled
//! over. When this happens, the chunk is closed (becomes read-only)
//! and stops taking writes. Any new writes to the same partition will
//! create a new active open chunk.
//!
//! Note: Strings in the mutable buffer are dictionary encoded (via
//! string interning) to reduce memory usage. This dictionary encoding
//! is done on a per-Chunk basis, so that as soon as the chunk is
//! closed the corresponding dictionary also becomes immutable

#![deny(broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod chunk;
mod column;
mod dictionary;
pub mod persistence_windows;
