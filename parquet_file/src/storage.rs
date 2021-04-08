/// This module responsible to write given data to specify object store and
/// read them back
use arrow_deps::{
    arrow::datatypes::SchemaRef,
    datafusion::physical_plan::SendableRecordBatchStream,
    parquet::{self, arrow::ArrowWriter, file::writer::TryClone},
};
use object_store::{
    path::{ObjectStorePath, Path},
    ObjectStore, ObjectStoreApi,
};

use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;
use snafu::{OptionExt, ResultExt, Snafu};
use std::{
    io::{Cursor, Seek, SeekFrom, Write},
    num::NonZeroU32,
    sync::Arc,
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error opening Parquet Writer: {}", source))]
    OpeningParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error reading stream while creating snapshot: {}", source))]
    ReadingStream {
        source: arrow_deps::arrow::error::ArrowError,
    },

    #[snafu(display("Error writing Parquet to memory: {}", source))]
    WritingParquetToMemory {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error closing Parquet Writer: {}", source))]
    ClosingParquetWriter {
        source: parquet::errors::ParquetError,
    },

    #[snafu(display("Error writing to object store: {}", source))]
    WritingToObjectStore { source: object_store::Error },

    #[snafu(display("Error converting to vec[u8]: Nothing else should have a reference here"))]
    WritingToMemWriter {},
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Storage {
    object_store: Arc<ObjectStore>,
    writer_id: NonZeroU32,
    db_name: String,
}

impl Storage {
    pub fn new(store: Arc<ObjectStore>, id: NonZeroU32, db: String) -> Self {
        Self {
            object_store: store,
            writer_id: id,
            db_name: db,
        }
    }

    /// Return full path including filename in the object store to save a chunk
    /// table file
    pub fn location(
        &self,
        partition_key: String,
        chunk_id: u32,
        table_name: String,
    ) -> object_store::path::Path {
        // Full path of the file in object store
        //    <writer id>/<database>/data/<partition key>/<chunk id>/<table
        // name>.parquet

        let mut path = self.object_store.new_path();
        path.push_dir(self.writer_id.to_string());
        path.push_dir(self.db_name.clone());
        path.push_dir("data");
        path.push_dir(partition_key);
        path.push_dir(chunk_id.to_string());
        let file_name = format!("{}.parquet", table_name);
        path.set_file_name(file_name);

        path
    }

    pub async fn write_to_object_store(
        &self,
        partition_key: String,
        chunk_id: u32,
        table_name: String,
        stream: SendableRecordBatchStream,
    ) -> Result<Path> {
        // Create full path location of this file in object store
        let path = self.location(partition_key, chunk_id, table_name);

        let schema = stream.schema();
        let data = Self::parquet_stream_to_bytes(stream, schema).await?;
        self.to_object_store(data, &path).await?;

        Ok(path.clone())
    }

    pub async fn parquet_stream_to_bytes(
        mut stream: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Result<Vec<u8>> {
        let mem_writer = MemWriter::default();
        {
            let mut writer = ArrowWriter::try_new(mem_writer.clone(), schema, None)
                .context(OpeningParquetWriter)?;
            while let Some(batch) = stream.next().await {
                let batch = batch.context(ReadingStream)?;
                writer.write(&batch).context(WritingParquetToMemory)?;
            }
            writer.close().context(ClosingParquetWriter)?;
        } // drop the reference to the MemWriter that the SerializedFileWriter has

        mem_writer.into_inner().context(WritingToMemWriter)
    }

    pub async fn to_object_store(
        &self,
        data: Vec<u8>,
        file_name: &object_store::path::Path,
    ) -> Result<()> {
        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        self.object_store
            .put(
                &file_name,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)
    }
}

#[derive(Debug, Default, Clone)]
pub struct MemWriter {
    mem: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl MemWriter {
    /// Returns the inner buffer as long as there are no other references to the
    /// Arc.
    pub fn into_inner(self) -> Option<Vec<u8>> {
        Arc::try_unwrap(self.mem)
            .ok()
            .map(|mutex| mutex.into_inner().into_inner())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.mem.lock();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.mem.lock();
        inner.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.mem.lock();
        inner.seek(pos)
    }
}

impl TryClone for MemWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            mem: Arc::clone(&self.mem),
        })
    }
}
