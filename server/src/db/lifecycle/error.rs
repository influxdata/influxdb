//! Errors that can occur during lifecycle actions
use snafu::Snafu;

use data_types::chunk_metadata::ChunkAddr;

use crate::db::catalog;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
// Export the snafu "selectors" so they can be used in other modules
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(context(false))]
    PartitionError { source: catalog::partition::Error },

    #[snafu(context(false))]
    ChunkError { source: catalog::chunk::Error },

    #[snafu(context(false))]
    PlannerError {
        source: query::frontend::reorg::Error,
    },

    #[snafu(context(false))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(context(false))]
    DataFusionError {
        source: datafusion::error::DataFusionError,
    },

    #[snafu(context(false))]
    Aborted { source: futures::future::Aborted },

    #[snafu(display("Read Buffer Error in chunk {}{} : {}", chunk_id, table_name, source))]
    ReadBufferChunkError {
        source: read_buffer::Error,
        table_name: String,
        chunk_id: u32,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Error while creating parquet chunk: {}", source))]
    ParquetChunkError { source: parquet_file::chunk::Error },

    #[snafu(display("Error while commiting transaction on preserved catalog: {}", source))]
    CommitError {
        source: parquet_file::catalog::core::Error,
    },

    #[snafu(display("Cannot write chunk: {}", addr))]
    CannotWriteChunk { addr: ChunkAddr },

    #[snafu(display("Cannot drop unpersisted chunk: {}", addr))]
    CannotDropUnpersistedChunk { addr: ChunkAddr },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
