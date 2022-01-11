//! Errors that can occur during lifecycle actions
use crate::catalog;
use data_types::chunk_metadata::ChunkAddr;
use snafu::Snafu;

#[allow(clippy::enum_variant_names)]
#[derive(Debug, Snafu)]
// Export the snafu "selectors" so they can be used in other modules
#[snafu(visibility(pub))]
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

    #[snafu(display("Error reading from object store: {}", source))]
    ReadingObjectStore {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore {
        source: parquet_file::storage::Error,
    },

    #[snafu(display("Error while creating parquet chunk: {}", source))]
    ParquetChunkError { source: parquet_file::chunk::Error },

    #[snafu(display("Error while commiting transaction on preserved catalog: {}", source))]
    CommitError {
        source: parquet_catalog::core::Error,
    },

    #[snafu(display("Cannot write chunk: {}", addr))]
    CannotWriteChunk { addr: ChunkAddr },

    #[snafu(display("Cannot drop unpersisted chunk: {}", addr))]
    CannotDropUnpersistedChunk { addr: ChunkAddr },

    #[snafu(display("No object store chunks provided for compacting"))]
    EmptyChunks {},

    #[snafu(display(
        "Cannot compact chunks because at least one does not belong to the given partition"
    ))]
    ChunksNotInPartition {},

    #[snafu(display("Cannot compact chunks because at least one is not yet persisted"))]
    ChunksNotPersisted {},

    #[snafu(display("Cannot compact the provided persisted chunks. They are not contiguous"))]
    ChunksNotContiguous {},

    #[snafu(display(
        "Error reading IOx Metadata from Parquet IoxParquetMetadata: {}",
        source
    ))]
    ParquetMetaRead {
        source: parquet_file::metadata::Error,
    },

    #[snafu(display("Cannot compact chunks because of error computing max partition checkpoint"))]
    ComparePartitionCheckpoint {},

    #[snafu(display("Cannot compact chunks because no checkpoint was computed"))]
    NoCheckpoint {},

    #[snafu(display("Cannot load chunk as no rows: {}", addr))]
    CannotLoadEmptyChunk { addr: ChunkAddr },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
