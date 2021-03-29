//! This module contains code for snapshotting a database chunk to Parquet
//! files in object storage.
use arrow_deps::{
    arrow::datatypes::SchemaRef,
    datafusion::physical_plan::SendableRecordBatchStream,
    parquet::{self, arrow::ArrowWriter, file::writer::TryClone},
};
use data_types::partition_metadata::{PartitionSummary, TableSummary};
use internal_types::selection::Selection;
use object_store::{path::ObjectStorePath, ObjectStore, ObjectStoreApi};
use query::{predicate::EMPTY_PREDICATE, PartitionChunk};

use std::{
    io::{Cursor, Seek, SeekFrom, Write},
    sync::Arc,
};

use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};
use tokio::sync::oneshot;
use tracing::{error, info};
use uuid::Uuid;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Partition error creating snapshot: {}", source))]
    PartitionError {
        source: Box<dyn std::error::Error + Send + Sync>,
    },

    #[snafu(display("Error reading stream while creating snapshot: {}", source))]
    ReadingStream {
        source: arrow_deps::arrow::error::ArrowError,
    },

    #[snafu(display("Table position out of bounds: {}", position))]
    TablePositionOutOfBounds { position: usize },

    #[snafu(display("Error generating json response: {}", source))]
    JsonGenerationError { source: serde_json::Error },

    #[snafu(display("Error opening Parquet Writer: {}", source))]
    OpeningParquetWriter {
        source: parquet::errors::ParquetError,
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

    #[snafu(display("Error reading batches while writing to '{}': {}", file_name, source))]
    ReadingBatches {
        file_name: String,
        source: arrow_deps::arrow::error::ArrowError,
    },

    #[snafu(display("Stopped early"))]
    StoppedEarly,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Snapshot<T>
where
    T: Send + Sync + 'static + PartitionChunk,
{
    pub id: Uuid,
    pub partition_summary: PartitionSummary,
    pub metadata_path: object_store::path::Path,
    pub data_path: object_store::path::Path,
    store: Arc<ObjectStore>,
    chunk: Arc<T>,
    status: Mutex<Status>,
}

impl<T> Snapshot<T>
where
    T: Send + Sync + 'static + PartitionChunk,
{
    fn new(
        partition_key: impl Into<String>,
        metadata_path: object_store::path::Path,
        data_path: object_store::path::Path,
        store: Arc<ObjectStore>,
        partition: Arc<T>,
        tables: Vec<TableSummary>,
    ) -> Self {
        let table_states = vec![TableState::NotStarted; tables.len()];

        let status = Status {
            table_states,
            ..Default::default()
        };

        Self {
            id: Uuid::new_v4(),
            partition_summary: PartitionSummary {
                key: partition_key.into(),
                tables,
            },
            metadata_path,
            data_path,
            store,
            chunk: partition,
            status: Mutex::new(status),
        }
    }

    // returns the position of the next table
    fn next_table(&self) -> Option<(usize, &str)> {
        let mut status = self.status.lock();

        status
            .table_states
            .iter()
            .position(|s| s == &TableState::NotStarted)
            .map(|pos| {
                status.table_states[pos] = TableState::Running;
                (pos, &*self.partition_summary.tables[pos].name)
            })
    }

    fn mark_table_finished(&self, position: usize) {
        let mut status = self.status.lock();

        if status.table_states.len() > position {
            status.table_states[position] = TableState::Finished;
        }
    }

    fn mark_meta_written(&self) {
        let mut status = self.status.lock();
        status.meta_written = true;
    }

    pub fn finished(&self) -> bool {
        let status = self.status.lock();

        status
            .table_states
            .iter()
            .all(|state| matches!(state, TableState::Finished))
    }

    fn should_stop(&self) -> bool {
        let status = self.status.lock();
        status.stop_on_next_update
    }

    async fn run(&self, notify: Option<oneshot::Sender<()>>) -> Result<()> {
        while let Some((pos, table_name)) = self.next_table() {
            // get all the data in this chunk:
            let stream = self
                .chunk
                .read_filter(table_name, &EMPTY_PREDICATE, Selection::All)
                .map_err(|e| Box::new(e) as _)
                .context(PartitionError)?;

            let schema = stream.schema();

            let mut location = self.data_path.clone();
            let file_name = format!("{}.parquet", table_name);
            location.set_file_name(&file_name);
            let data = Self::parquet_stream_to_bytes(stream, schema).await?;
            self.write_to_object_store(data, &location).await?;
            self.mark_table_finished(pos);

            if self.should_stop() {
                return StoppedEarly.fail();
            }
        }

        let mut partition_meta_path = self.metadata_path.clone();
        let key = format!("{}.json", &self.partition_summary.key);
        partition_meta_path.set_file_name(&key);
        let json_data = serde_json::to_vec(&self.partition_summary).context(JsonGenerationError)?;
        let data = Bytes::from(json_data);
        let len = data.len();
        let stream_data = std::io::Result::Ok(data);
        self.store
            .put(
                &partition_meta_path,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)?;

        self.mark_meta_written();

        if let Some(notify) = notify {
            if let Err(e) = notify.send(()) {
                error!("error sending notify: {:?}", e);
            }
        }

        Ok(())
    }

    /// Convert the record batches in stream to bytes in a parquet file stream
    /// in memory
    ///
    /// TODO: connect the streams to avoid buffering into Vec<u8>
    async fn parquet_stream_to_bytes(
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

        Ok(mem_writer
            .into_inner()
            .expect("Nothing else should have a reference here"))
    }

    async fn write_to_object_store(
        &self,
        data: Vec<u8>,
        file_name: &object_store::path::Path,
    ) -> Result<()> {
        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        self.store
            .put(
                &file_name,
                futures::stream::once(async move { stream_data }),
                Some(len),
            )
            .await
            .context(WritingToObjectStore)
    }

    fn set_error(&self, e: Error) {
        let mut status = self.status.lock();
        status.error = Some(e);
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum TableState {
    NotStarted,
    Running,
    Finished,
}

#[derive(Debug, Default)]
pub struct Status {
    table_states: Vec<TableState>,
    meta_written: bool,
    stop_on_next_update: bool,
    error: Option<Error>,
}

pub fn snapshot_chunk<T>(
    metadata_path: object_store::path::Path,
    data_path: object_store::path::Path,
    store: Arc<ObjectStore>,
    partition_key: &str,
    chunk: Arc<T>,
    notify: Option<oneshot::Sender<()>>,
) -> Result<Arc<Snapshot<T>>>
where
    T: Send + Sync + 'static + PartitionChunk,
{
    let table_stats = chunk
        .table_stats()
        .map_err(|e| Box::new(e) as _)
        .context(PartitionError)?;

    let snapshot = Snapshot::new(
        partition_key.to_string(),
        metadata_path,
        data_path,
        store,
        chunk,
        table_stats,
    );
    let snapshot = Arc::new(snapshot);

    let return_snapshot = Arc::clone(&snapshot);

    tokio::spawn(async move {
        info!(
            "starting snapshot of {} to {}",
            &snapshot.partition_summary.key,
            &snapshot.data_path.display()
        );
        if let Err(e) = snapshot.run(notify).await {
            error!("error running snapshot: {:?}", e);
            snapshot.set_error(e);
        }
    });

    Ok(return_snapshot)
}

#[derive(Debug, Default, Clone)]
struct MemWriter {
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

#[cfg(test)]
mod tests {
    use crate::{
        db::{DBChunk, Db},
        JobRegistry,
    };
    use read_buffer::Database as ReadBufferDb;

    use super::*;
    use data_types::database_rules::DatabaseRules;
    use futures::TryStreamExt;
    use mutable_buffer::{chunk::Chunk as ChunkWB, MutableBufferDb};
    use object_store::memory::InMemory;
    use query::{test::TestLPWriter, Database};

    #[tokio::test]
    async fn snapshot() {
        let lp = r#"
cpu,host=A,region=west user=23.2,system=55.1 1
cpu,host=A,region=west user=3.2,system=50.1 10
cpu,host=B,region=east user=10.0,system=74.1 1
mem,host=A,region=west used=45 1
        "#;

        let db = make_db();
        let mut writer = TestLPWriter::default();
        writer.write_lp_string(&db, &lp).unwrap();

        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut metadata_path = store.new_path();
        metadata_path.push_dir("meta");

        let mut data_path = store.new_path();
        data_path.push_dir("data");

        let chunk = Arc::clone(&db.chunks("1970-01-01T00")[0]);

        let snapshot = snapshot_chunk(
            metadata_path.clone(),
            data_path,
            Arc::clone(&store),
            "testaroo",
            chunk,
            Some(tx),
        )
        .unwrap();

        rx.await.unwrap();

        let mut location = metadata_path;
        location.set_file_name("testaroo.json");

        let summary = store
            .get(&location)
            .await
            .unwrap()
            .map_ok(|b| bytes::BytesMut::from(&b[..]))
            .try_concat()
            .await
            .unwrap();

        let meta: PartitionSummary = serde_json::from_slice(&*summary).unwrap();
        assert_eq!(meta, snapshot.partition_summary);
    }

    #[test]
    fn snapshot_states() {
        let tables = vec![
            TableSummary {
                name: "foo".to_string(),
                columns: vec![],
            },
            TableSummary {
                name: "bar".to_string(),
                columns: vec![],
            },
            TableSummary {
                name: "asdf".to_string(),
                columns: vec![],
            },
        ];

        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let chunk = DBChunk::new_mb(Arc::new(ChunkWB::new(11)), "key", false);
        let mut metadata_path = store.new_path();
        metadata_path.push_dir("meta");

        let mut data_path = store.new_path();
        data_path.push_dir("data");

        let snapshot = Snapshot::new("testaroo", metadata_path, data_path, store, chunk, tables);

        let (pos, name) = snapshot.next_table().unwrap();
        assert_eq!(0, pos);
        assert_eq!("foo", name);

        let (pos, name) = snapshot.next_table().unwrap();
        assert_eq!(1, pos);
        assert_eq!("bar", name);

        snapshot.mark_table_finished(1);
        assert!(!snapshot.finished());

        let (pos, name) = snapshot.next_table().unwrap();
        assert_eq!(2, pos);
        assert_eq!("asdf", name);

        assert!(snapshot.next_table().is_none());
        assert!(!snapshot.finished());

        snapshot.mark_table_finished(0);
        snapshot.mark_table_finished(2);
        assert!(snapshot.finished());
    }

    /// Create a Database with a local store
    pub fn make_db() -> Db {
        let name = "test_db";
        Db::new(
            DatabaseRules::new(),
            Some(MutableBufferDb::new(name)),
            ReadBufferDb::new(),
            None, // wal buffer
            Arc::new(JobRegistry::new()),
        )
    }
}
