//! IOx Ingester V2 implementation.
//!
//! # Overview
//!
//! The purpose of the ingester is to receive RPC write requests, and batch them
//! up until ready to be persisted. This buffered data must be durable (tolerant
//! of a crash) and queryable via the Flight RPC query interface. Data is
//! organised in a hierarchical structure (the [`BufferTree`]) to allow
//! table-scoped queries, and partition-scoped persistence / parquet file
//! generation.
//!
//! All buffered data is periodically persisted via a WAL file rotation, or
//! selectively when a single partition has grown too large ("hot partition
//! persistence").
//!
//! ```text
//!
//!                ┌──────────────┐        ┌──────────────┐
//!                │  RPC Write   │        │  RPC Query   │
//!                └──────────────┘        └──────────────┘
//!                        │                       ▲
//!                        ▼                       │
//!                ┌──────────────┐                │
//!                │     WAL      │                │
//!                └──────────────┘                │
//!                        │                       │
//!                        │   ┌──────────────┐    │
//!                        └──▶│  BufferTree  │────┘
//!                            └──────────────┘
//!                                    │
//!                                    ▼
//!                            ┌──────────────┐
//!                            │   Persist    │
//!                            └──────────────┘
//!                                    │
//!                                    ▼
//!
//!                             Object Storage
//!
//!
//!
//!                                                   (arrows show data flow)
//!```
//!
//! The write path is composed together of implementers of the [`DmlSink`]
//! abstraction, and likewise queries stream out of the [`QueryExec`]
//! abstraction.
//!
//!
//! ## Subsystems
//!
//! The Ingester is composed of multiple smaller, distinct subsystems that
//! communicate / work together to provide the full ingester functionality.
//!
//! Each subsystem has its own documentation further describing the behaviours
//! and problems it solves in more detail.
//!
//!
//! ### [`BufferTree`]
//!
//! Perhaps not a "system" as such, but a single instance of the [`BufferTree`]
//! is the central point of the ingester - all writes are buffered in the tree,
//! and all queries execute against it. Persist operations persist data in the
//! buffer tree, removing it once complete.
//!
//! All other systems either directly or indirectly operate on, or control
//! operations against the [`BufferTree`].
//!
//!
//! ### RPC Server
//!
//! External services communicate with the ingester via gRPC service calls. All
//! gRPC handlers can be found in the [`grpc`] modules.
//!
//! The two main RPC endpoints are:
//!
//!  * Write: buffer new data in the ingester, issued by the router
//!  * Query: return buffered data scoped by {namespace, table}, issued by the
//!           queriers
//!
//! Both endpoints are latency sensitive, and the call durations are directly
//! observable by end users.
//!
//! Writes commit to the [`wal`] for durability / crash tolerance and are
//! buffered into the [`BufferTree`] in parallel. Queries execute against the
//! [`BufferTree`], lazily streaming the results back to the queriers (lock
//! acquisition and data copying is deferred until "pulled" by the querier).
//!
//! If the [`IngestState`] is marked as unhealthy/shutting down, then write
//! requests are rejected with a "resource exhausted" error message until it
//! becomes healthy again.
//!
//!
//! ### Persist System
//!
//! The persist system is responsible for durably writing data to object
//! storage; that includes compacting it (to remove duplicate/overwrote rows),
//! generating a parquet file, uploading it to the configured object store, and
//! inserting the necessary catalog state to make the new file queryable.
//!
//! Data is typically sourced from a [`BufferTree`], and once the new file is
//! queryable, the data it contains is removed from the [`BufferTree`].
//!
//! Code that uses the persist system does so through the [`PersistQueue`]
//! abstraction, implemented by the [`PersistHandle`].
//!
//! The persist system provides a logical queue of outstanding persist jobs, and
//! a configurable number of worker tasks to execute them - see
//! [`PersistHandle`] for detailed documentation. If the persist system is
//! "saturated" (queue depth reached maximum) then further writes are rejected
//! by setting the [`IngestState`] to [`IngestStateError::PersistSaturated`]
//! until the queue depth is reduced.
//!
//! The persist system is driven mainly by WAL rotation (below), and interacts
//! with the [`IngestState`] to provide back-pressure, indirectly controlling
//! the memory utilisation of the Ingester (to prevent OOMs). Partitions with
//! large volumes of writes (or otherwise problematic data) are prematurely
//! persisted independently of the WAL rotation ("hot partition persistence").
//!
//!
//! ### WAL
//!
//! The write-ahead log ([`wal`]) is used to durably record each operation
//! against the [`BufferTree`] into a replay log, with a partial order. This
//! happens synchronously in the hot write path, so WAL performance is a
//! critical consideration.
//!
//! Once a write has been buffered in the [`BufferTree`] AND flushed to disk in
//! the [`wal`], the request is ACKed to the user.
//!
//! If the ingester crashes / stops un-cleanly, then the WAL must be replayed to
//! rebuild the in-memory state (the [`BufferTree`]) to prevent data loss. This
//! has some quirks, discussed in "Write Reordering" below.
//!
//!
//! #### WAL Rotation
//!
//! The WAL file is periodically rotated at a configurable interval, which
//! triggers a full persistence of all buffered data in the ingester at the same
//! time. This "full persist" indirectly affects the ingester in many ways:
//!
//!   * Limits the size of a single WAL file
//!   * Limits the amount of buffered data, which in turn
//!     * Limits the amount of WAL data to replay after a crash
//!     * Limits the amount of data a querier must read & dedupe per query
//!     * Limits the largest source of memory utilisation in the ingester
//!     * Limits the amount of data in a partition that must be persisted
//!
//!
//! ## Write Reordering
//!
//! A write that enters an `ingester` instance can be reordered arbitrarily
//! with concurrent write requests.
//!
//! For example, two gRPC writes can race to be committed to the [`wal`], and
//! then race again to be buffered into the [`BufferTree`]. Writes to a
//! [`BufferTree`] may arrive out-of-order w.r.t their assigned sequence
//! numbers.
//!
//! This can also lead to the ordering of entries in the [`wal`] diverging from
//! the order of ops applied to the [`BufferTree`] (see
//! <https://github.com/influxdata/influxdb_iox/issues/6276>).
//!
//! This requires careful management, but ultimately allows for high levels of
//! parallelism when handling both writes and queries, increasing the
//! performance of both.
//!
//! Because of re-ordering, ranges of [`SequenceNumber`] cannot be used to
//! indirectly equality match (nor prefix match) the underlying data;
//! non-monotonic writes means overlapping ranges do not guarantee equality of
//! the set of operations they represent (gaps may be present). For example, a
//! range of sequence numbers bounded by `[0, 2)` for thread 1 may not contain
//! the same op data as another thread T2 with range with the same bounds due to
//! reordering causing T1 to observe `{0, 1, 2}` and T2 observing `{0, 2}` and
//! after a faulty range comparison, `{1}` to converge.
//!
//! [`BufferTree`]: crate::buffer_tree::BufferTree
//! [`SequenceNumber`]: data_types::SequenceNumber
//! [`PersistQueue`]: crate::persist::queue::PersistQueue
//! [`PersistHandle`]: crate::persist::handle::PersistHandle
//! [`IngestState`]: crate::ingest_state::IngestState
//! [`grpc`]: crate::server::grpc
//! [`DmlSink`]: crate::dml_sink::DmlSink
//! [`QueryExec`]: crate::query::QueryExec
//! [`IngestStateError::PersistSaturated`]:
//!     crate::ingest_state::IngestStateError

#![allow(dead_code)] // Until ingester2 is used.
#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    clippy::clone_on_ref_ptr,
    clippy::dbg_macro,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::todo,
    clippy::use_self,
    missing_copy_implementations,
    missing_debug_implementations,
    unused_crate_dependencies,
    missing_docs
)]
#![allow(clippy::default_constructed_unit_structs)]

// Workaround for "unused crate" lint false positives.
#[cfg(test)]
use criterion as _;
#[cfg(test)]
use influxdb_iox_client as _;
#[cfg(test)]
use ingester_test_ctx as _;
#[cfg(test)]
use itertools as _;
use workspace_hack as _;

/// Ingester initialisation methods & types.
///
/// This module defines the public API boundary of the Ingester crate.
mod init;
pub use init::*;

//
// !!! PLEASE DO NOT EXPORT !!!
//
// Please be judicious when deciding if something should be visible outside this
// crate. Overzealous use of `pub` exposes the internals of an Ingester and
// causes tight coupling with other components. I know it's tempting, but it
// causes problems in the long run.
//
// Ideally anything that NEEDS to be shared with other components happens via a
// shared common crate. Any external components should interact with an Ingester
// through its public API only, and not by poking around at the internals.

/// This module needs to be pub for the benchmarks but should not be used outside the crate.
#[cfg(feature = "benches")]
pub mod internal_implementation_details {
    pub use super::buffer_tree::*;
    pub use super::dml_payload::*;
    pub use super::dml_sink::*;
    pub use super::partition_iter::*;
    pub use super::persist::*;
}

mod arcmap;
mod buffer_tree;
mod cancellation_safe;
mod deferred_load;
mod dml_payload;
mod dml_sink;
mod ingest_state;
mod ingester_id;
mod partition_iter;
mod persist;
mod query;
mod query_adaptor;
pub(crate) mod server;
mod timestamp_oracle;
mod wal;

#[cfg(test)]
mod test_util;
