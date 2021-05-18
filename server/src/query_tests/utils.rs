use data_types::{
    chunk_metadata::{ChunkStorage, ChunkSummary},
    database_rules::{DatabaseRules, WriteBufferRollover},
    server_id::ServerId,
    DatabaseName,
};
use object_store::{memory::InMemory, ObjectStore};
use query::{exec::Executor, Database};

use crate::{
    buffer::Buffer,
    db::{load_preserved_catalog, Db},
    JobRegistry,
};
use std::{borrow::Cow, convert::TryFrom, sync::Arc};

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

    pub async fn build(self) -> TestDb {
        let server_id = self
            .server_id
            .unwrap_or_else(|| ServerId::try_from(1).unwrap());
        let db_name = self
            .db_name
            .unwrap_or_else(|| DatabaseName::new("placeholder").unwrap());
        let object_store = self
            .object_store
            .unwrap_or_else(|| Arc::new(ObjectStore::new_in_memory(InMemory::new())));

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
        let preserved_catalog = load_preserved_catalog(
            db_name.as_str(),
            Arc::clone(&object_store),
            server_id,
            Arc::clone(&metrics_registry),
        )
        .await
        .unwrap();

        TestDb {
            metric_registry: metrics::TestMetricRegistry::new(metrics_registry),
            db: Db::new(
                DatabaseRules::new(db_name),
                server_id,
                object_store,
                exec,
                write_buffer,
                Arc::new(JobRegistry::new()),
                preserved_catalog,
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
pub async fn make_db() -> TestDb {
    TestDb::builder().build().await
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
