//! Errors that can occur during lifecycle actions
use data_types::chunk_metadata::ChunkAddr;
use snafu::Snafu;

#[derive(Debug, Snafu)]
// Export the snafu "selectors" so they can be used in other modules
#[snafu(visibility = "pub")]
pub enum Error {
    #[snafu(context(false))]
    PartitionError {
        source: crate::db::catalog::partition::Error,
    },

    #[snafu(display("Lifecycle error: {}", source))]
    LifecycleError {
        source: crate::db::catalog::chunk::Error,
    },

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

    #[snafu(display("Error sending entry to write buffer"))]
    WriteBufferError {
        source: Box<dyn std::error::Error + Sync + Send>,
    },

    #[snafu(display("Error while creating parquet chunk: {}", source))]
    ParquetChunkError { source: parquet_file::chunk::Error },

    #[snafu(display("Error while handling transaction on preserved catalog: {}", source))]
    TransactionError {
        source: parquet_file::catalog::Error,
    },

    #[snafu(display("Error while commiting transaction on preserved catalog: {}", source))]
    CommitError {
        source: parquet_file::catalog::Error,
    },

    #[snafu(display("Cannot write chunk: {}", addr))]
    CannotWriteChunk { addr: ChunkAddr },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
