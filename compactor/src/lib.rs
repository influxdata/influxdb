//! IOx compactor implementation.

#![deny(rustdoc::broken_intra_doc_links, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_docs,
    clippy::explicit_iter_loop,
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr
)]

pub mod compact;
pub mod garbage_collector;
pub mod handler;
pub(crate) mod parquet_file_filtering;
pub(crate) mod parquet_file_lookup;
pub mod query;
pub mod server;
pub mod utils;

use crate::compact::Compactor;
use data_types::PartitionId;
use snafu::{ResultExt, Snafu};
use std::sync::Arc;

#[derive(Debug, Snafu)]
#[allow(missing_copy_implementations, missing_docs)]
pub(crate) enum Error {
    #[snafu(display("{}", source))]
    ParquetFileLookup {
        source: parquet_file_lookup::PartitionFilesFromPartitionError,
    },
}

/// Eventually what should be called for each partition selected for compaction
#[allow(dead_code)]
pub(crate) async fn compact_partition(
    compactor: &Compactor,
    partition_id: PartitionId,
) -> Result<(), Error> {
    let parquet_files_for_compaction =
        parquet_file_lookup::ParquetFilesForCompaction::for_partition(
            Arc::clone(&compactor.catalog),
            partition_id,
        )
        .await
        .context(ParquetFileLookupSnafu)?;

    let _to_compact = parquet_file_filtering::filter_parquet_files(
        parquet_files_for_compaction,
        compactor.config.input_size_threshold_bytes(),
        compactor.config.input_file_count_threshold(),
        &compactor.parquet_file_candidate_gauge,
        &compactor.parquet_file_candidate_bytes_gauge,
    );

    // TODO:
    // compact(to_compact)
    unimplemented!("actually compacting the selected parquet files");
}
