use data_types::{
    chunk::{ChunkStorage, ChunkSummary},
    database_rules::{DatabaseRules, WriteBufferRollover},
    server_id::ServerId,
    DatabaseName,
};
use object_store::{disk::File, ObjectStore};
use query::{exec::Executor, Database};

use crate::{buffer::Buffer, db::Db, JobRegistry};
use std::{borrow::Cow, convert::TryFrom, sync::Arc};
use tempfile::TempDir;

// A wrapper around a Db and a metrics registry allowing for isolated testing
// of a Db and its metrics.
#[derive(Debug)]
pub struct TestDb {
    pub db: Db,
    pub metric_registry: metrics::TestMetricRegistry,
}

impl TestDb {
    pub fn builder() -> TestDbBuilder {
        TestDbBuilder::new()
    }
}

#[derive(Debug, Default)]
pub struct TestDbBuilder {
    server_id: Option<ServerId>,
    object_store: Option<Arc<ObjectStore>>,
    db_name: Option<DatabaseName<'static>>,
    write_buffer: bool,
}

impl TestDbBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn build(self) -> TestDb {
        let server_id = self
            .server_id
            .unwrap_or_else(|| ServerId::try_from(1).unwrap());
        let db_name = self
            .db_name
            .unwrap_or_else(|| DatabaseName::new("placeholder").unwrap());

        // TODO: When we support parquet file in memory, we will either turn this test back to
        // memory or have both tests: local disk and memory
        //let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
        //
        // Unless otherwise specified, create an object store with a specified location in a local
        // disk
        let object_store = self.object_store.unwrap_or_else(|| {
            let root = TempDir::new().unwrap();
            Arc::new(ObjectStore::new_file(File::new(root.path())))
        });

        let exec = Arc::new(Executor::new(1));
        let metrics_registry = Arc::new(metrics::MetricRegistry::new());

        let write_buffer = if self.write_buffer {
            let max = 1 << 32;
            let segment = 1 << 16;
            Some(Buffer::new(
                max,
                segment,
                WriteBufferRollover::ReturnError,
                false,
                server_id,
            ))
        } else {
            None
        };

        TestDb {
            metric_registry: metrics::TestMetricRegistry::new(Arc::clone(&metrics_registry)),
            db: Db::new(
                DatabaseRules::new(db_name),
                server_id,
                object_store,
                exec,
                write_buffer,
                Arc::new(JobRegistry::new()),
                metrics_registry,
            ),
        }
    }

    pub fn server_id(mut self, server_id: ServerId) -> Self {
        self.server_id = Some(server_id);
        self
    }

    pub fn object_store(mut self, object_store: Arc<ObjectStore>) -> Self {
        self.object_store = Some(object_store);
        self
    }

    pub fn db_name<T: Into<Cow<'static, str>>>(mut self, db_name: T) -> Self {
        self.db_name = Some(DatabaseName::new(db_name).unwrap());
        self
    }

    pub fn write_buffer(mut self, enabled: bool) -> Self {
        self.write_buffer = enabled;
        self
    }
}

/// Used for testing: create a Database with a local store
pub fn make_db() -> TestDb {
    TestDb::builder().build()
}

fn chunk_summary_iter(db: &Db) -> impl Iterator<Item = ChunkSummary> + '_ {
    db.partition_keys()
        .unwrap()
        .into_iter()
        .flat_map(move |partition_key| db.partition_chunk_summaries(&partition_key))
}

/// Returns the number of mutable buffer chunks in the specified database
pub fn count_mutable_buffer_chunks(db: &Db) -> usize {
    chunk_summary_iter(db)
        .filter(|s| {
            s.storage == ChunkStorage::OpenMutableBuffer
                || s.storage == ChunkStorage::ClosedMutableBuffer
        })
        .count()
}

/// Returns the number of read buffer chunks in the specified database
pub fn count_read_buffer_chunks(db: &Db) -> usize {
    chunk_summary_iter(db)
        .filter(|s| {
            s.storage == ChunkStorage::ReadBuffer
                || s.storage == ChunkStorage::ReadBufferAndObjectStore
        })
        .count()
}

/// Returns the number of object store chunks in the specified database
pub fn count_object_store_chunks(db: &Db) -> usize {
    chunk_summary_iter(db)
        .filter(|s| {
            s.storage == ChunkStorage::ReadBufferAndObjectStore
                || s.storage == ChunkStorage::ObjectStoreOnly
        })
        .count()
}
