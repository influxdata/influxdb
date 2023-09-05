//! IOx test utils and tests

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

use data_types::{PartitionKey, TableId, TransitionPartitionId};

mod catalog;
pub use catalog::{
    TestCatalog, TestNamespace, TestParquetFile, TestParquetFileBuilder, TestPartition, TestTable,
};

mod builders;
pub use builders::{ParquetFileBuilder, PartitionBuilder, SkippedCompactionBuilder, TableBuilder};

/// Create a partition identifier from an int (which gets used as the table ID) and a partition key
/// with the string "arbitrary". Most useful in cases where there isn't any actual catalog
/// interaction (that is, in mocks) and when the important property of the partition identifiers is
/// that they're either the same or different than other partition identifiers.
pub fn partition_identifier(table_id: i64) -> TransitionPartitionId {
    TransitionPartitionId::new(TableId::new(table_id), &PartitionKey::from("arbitrary"))
}
