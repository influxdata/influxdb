//! This module contains code for snapshotting a database chunk to Parquet
//! files in object storage.
use arrow_deps::{
    arrow::record_batch::RecordBatch,
    parquet::{self, arrow::ArrowWriter, file::writer::TryClone},
};
use data_types::partition_metadata::{Partition as PartitionMeta, Table};
use object_store::{path::ObjectStorePath, ObjectStore};
use query::{selection::Selection, PartitionChunk};

use std::io::{Cursor, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
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
    pub partition_meta: PartitionMeta,
    pub metadata_path: ObjectStorePath,
    pub data_path: ObjectStorePath,
    store: Arc<ObjectStore>,
    partition: Arc<T>,
    status: Mutex<Status>,
}

impl<T> Snapshot<T>
where
    T: Send + Sync + 'static + PartitionChunk,
{
    fn new(
        partition_key: impl Into<String>,
        metadata_path: ObjectStorePath,
        data_path: ObjectStorePath,
        store: Arc<ObjectStore>,
        partition: Arc<T>,
        tables: Vec<Table>,
    ) -> Self {
        let table_states = vec![TableState::NotStarted; tables.len()];

        let status = Status {
            table_states,
            ..Default::default()
        };

        Self {
            id: Uuid::new_v4(),
            partition_meta: PartitionMeta {
                key: partition_key.into(),
                tables,
            },
            metadata_path,
            data_path,
            store,
            partition,
            status: Mutex::new(status),
        }
    }

    fn data_path(&self) -> String {
        self.store.convert_path(&self.data_path)
    }

    // returns the position of the next table
    fn next_table(&self) -> Option<(usize, &str)> {
        let mut status = self.status.lock().expect("mutex poisoned");

        status
            .table_states
            .iter()
            .position(|s| s == &TableState::NotStarted)
            .map(|pos| {
                status.table_states[pos] = TableState::Running;
                (pos, &*self.partition_meta.tables[pos].name)
            })
    }

    fn mark_table_finished(&self, position: usize) {
        let mut status = self.status.lock().expect("mutex poisoned");

        if status.table_states.len() > position {
            status.table_states[position] = TableState::Finished;
        }
    }

    fn mark_meta_written(&self) {
        let mut status = self.status.lock().expect("mutex poisoned");
        status.meta_written = true;
    }

    pub fn finished(&self) -> bool {
        let status = self.status.lock().expect("mutex poisoned");

        status
            .table_states
            .iter()
            .all(|state| matches!(state, TableState::Finished))
    }

    fn should_stop(&self) -> bool {
        let status = self.status.lock().expect("mutex poisoned");
        status.stop_on_next_update
    }

    async fn run(&self, notify: Option<oneshot::Sender<()>>) -> Result<()> {
        while let Some((pos, table_name)) = self.next_table() {
            let mut batches = Vec::new();
            self.partition
                .table_to_arrow(&mut batches, table_name, Selection::All)
                .map_err(|e| Box::new(e) as _)
                .context(PartitionError)?;

            let mut location = self.data_path.clone();
            let file_name = format!("{}.parquet", table_name);
            location.set_file_name(&file_name);
            self.write_batches(batches, &location).await?;
            self.mark_table_finished(pos);

            if self.should_stop() {
                return StoppedEarly.fail();
            }
        }

        let mut partition_meta_path = self.metadata_path.clone();
        let key = format!("{}.json", &self.partition_meta.key);
        partition_meta_path.set_file_name(&key);
        let json_data = serde_json::to_vec(&self.partition_meta).context(JsonGenerationError)?;
        let data = Bytes::from(json_data);
        let len = data.len();
        let stream_data = std::io::Result::Ok(data);
        self.store
            .put(
                &partition_meta_path,
                futures::stream::once(async move { stream_data }),
                len,
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

    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        file_name: &ObjectStorePath,
    ) -> Result<()> {
        let mem_writer = MemWriter::default();
        {
            let mut writer = ArrowWriter::try_new(mem_writer.clone(), batches[0].schema(), None)
                .context(OpeningParquetWriter)?;
            for batch in batches.into_iter() {
                writer.write(&batch).context(WritingParquetToMemory)?;
            }
            writer.close().context(ClosingParquetWriter)?;
        } // drop the reference to the MemWriter that the SerializedFileWriter has

        let data = mem_writer
            .into_inner()
            .expect("Nothing else should have a reference here");

        let len = data.len();
        let data = Bytes::from(data);
        let stream_data = Result::Ok(data);

        self.store
            .put(
                &file_name,
                futures::stream::once(async move { stream_data }),
                len,
            )
            .await
            .context(WritingToObjectStore)
    }

    fn set_error(&self, e: Error) {
        let mut status = self.status.lock().expect("mutex poisoned");
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
    metadata_path: ObjectStorePath,
    data_path: ObjectStorePath,
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

    let return_snapshot = snapshot.clone();

    tokio::spawn(async move {
        info!(
            "starting snapshot of {} to {}",
            &snapshot.partition_meta.key,
            &snapshot.data_path()
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
            .and_then(|mutex| mutex.into_inner().ok())
            .map(|cursor| cursor.into_inner())
    }
}

impl Write for MemWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.mem.lock().unwrap();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.mem.lock().unwrap();
        inner.flush()
    }
}

impl Seek for MemWriter {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.mem.lock().unwrap();
        inner.seek(pos)
    }
}

impl TryClone for MemWriter {
    fn try_clone(&self) -> std::io::Result<Self> {
        Ok(Self {
            mem: self.mem.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::data::lines_to_replicated_write;
    use data_types::database_rules::DatabaseRules;
    use futures::TryStreamExt;
    use influxdb_line_protocol::parse_lines;
    use mutable_buffer::chunk::Chunk as ChunkWB;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn snapshot() {
        let lp = r#"
cpu,host=A,region=west user=23.2,system=55.1 1
cpu,host=A,region=west user=3.2,system=50.1 10
cpu,host=B,region=east user=10.0,system=74.1 1
mem,host=A,region=west used=45 1
        "#;

        let lines: Vec<_> = parse_lines(lp).map(|l| l.unwrap()).collect();
        let write = lines_to_replicated_write(1, 1, &lines, &DatabaseRules::default());
        let mut chunk = ChunkWB::new(11);

        for e in write.write_buffer_batch().unwrap().entries().unwrap() {
            chunk.write_entry(&e).unwrap();
        }

        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let chunk = Arc::new(chunk);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let mut metadata_path = ObjectStorePath::default();
        metadata_path.push_dir("meta");

        let mut data_path = ObjectStorePath::default();
        data_path.push_dir("data");

        let snapshot = snapshot_chunk(
            metadata_path.clone(),
            data_path,
            store.clone(),
            "testaroo",
            chunk.clone(),
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

        let meta: PartitionMeta = serde_json::from_slice(&*summary).unwrap();
        assert_eq!(meta, snapshot.partition_meta);
    }

    #[test]
    fn snapshot_states() {
        let tables = vec![
            Table {
                name: "foo".to_string(),
                columns: vec![],
            },
            Table {
                name: "bar".to_string(),
                columns: vec![],
            },
            Table {
                name: "asdf".to_string(),
                columns: vec![],
            },
        ];

        let store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        let chunk = Arc::new(ChunkWB::new(11));
        let mut metadata_path = ObjectStorePath::default();
        metadata_path.push_dir("meta");

        let mut data_path = ObjectStorePath::default();
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
}
