//! The IOx catalog which keeps track of what namespaces, tables, columns, parquet files,
//! and deletes are in the system. Configuration information for distributing ingest, query
//! and compaction is also stored here.
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

const SHARED_KAFKA_TOPIC: &str = "iox_shared";
const SHARED_QUERY_POOL: &str = SHARED_KAFKA_TOPIC;
const TIME_COLUMN: &str = "time";

pub mod postgres;
