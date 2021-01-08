use query::PartitionChunk;
use snafu::{ResultExt, Snafu};

use std::sync::Arc;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Mutable Buffer Chunk Error: {}", source))]
    MutableBufferChunk {
        source: mutable_buffer::chunk::Error,
    },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A IOx DatabaseChunk can come from one of three places:
/// MutableBuffer, ReadBuffer, or a ParquetFile
#[derive(Debug)]
pub enum DBChunk {
    MutableBuffer(Arc<mutable_buffer::chunk::Chunk>),
    ReadBuffer,  // TODO add appropriate type here
    ParquetFile, // TODO add appropriate type here
}

impl PartitionChunk for DBChunk {
    type Error = Error;

    fn id(&self) -> u64 {
        match self {
            Self::MutableBuffer(chunk) => chunk.id(),
            Self::ReadBuffer => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_stats(&self) -> Result<Vec<data_types::partition_metadata::Table>, Self::Error> {
        match self {
            Self::MutableBuffer(chunk) => chunk.table_stats().context(MutableBufferChunk),
            Self::ReadBuffer => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }

    fn table_to_arrow(
        &self,
        dst: &mut Vec<arrow_deps::arrow::record_batch::RecordBatch>,
        table_name: &str,
        columns: &[&str],
    ) -> Result<(), Self::Error> {
        match self {
            Self::MutableBuffer(chunk) => chunk
                .table_to_arrow(dst, table_name, columns)
                .context(MutableBufferChunk),
            Self::ReadBuffer => unimplemented!("read buffer not implemented"),
            Self::ParquetFile => unimplemented!("parquet file not implemented"),
        }
    }
}
