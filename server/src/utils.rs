use std::{borrow::Cow, convert::TryFrom, num::NonZeroU64, sync::Arc, time::Duration};

use data_types::{
    chunk_metadata::{ChunkStorage, ChunkSummary},
    database_rules::{DatabaseRules, PartitionTemplate, TemplatePart},
    server_id::ServerId,
    DatabaseName,
};
use object_store::{memory::InMemory, ObjectStore};
use query::{exec::Executor, QueryDatabase};

use crate::{
    db::{load::load_or_create_preserved_catalog, DatabaseToCommit, Db},
    write_buffer::WriteBufferWriting,
    JobRegistry,
};

// A wrapper around a Db and a metrics registry allowing for isolated testing
// of a Db and its metrics.
#[derive(Debug)]
pub struct TestDb {
    pub db: Arc<Db>,
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
    worker_cleanup_avg_sleep: Option<Duration>,
    write_buffer: Option<Arc<dyn WriteBufferWriting>>,
    catalog_transactions_until_checkpoint: Option<NonZeroU64>,
    partition_template: Option<PartitionTemplate>,
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

        let (preserved_catalog, catalog) = load_or_create_preserved_catalog(
            db_name.as_str(),
            Arc::clone(&object_store),
            server_id,
            Arc::clone(&metrics_registry),
            true,
        )
        .await
        .unwrap();

        let mut rules = DatabaseRules::new(db_name);

        // make background loop spin a bit faster for tests
        rules.worker_cleanup_avg_sleep = self
            .worker_cleanup_avg_sleep
            .unwrap_or_else(|| Duration::from_secs(1));

        // enable checkpointing
        if let Some(v) = self.catalog_transactions_until_checkpoint {
            rules.lifecycle_rules.catalog_transactions_until_checkpoint = v;
        }

        // set partion template
        if let Some(partition_template) = self.partition_template {
            rules.partition_template = partition_template;
        } else {
            // default to hourly
            rules.partition_template = PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("%Y-%m-%dT%H".to_string())],
            };
        }

        let database_to_commit = DatabaseToCommit {
            rules,
            server_id,
            object_store,
            preserved_catalog,
            catalog,
            write_buffer: self.write_buffer,
            exec,
        };

        TestDb {
            metric_registry: metrics::TestMetricRegistry::new(metrics_registry),
            db: Arc::new(Db::new(database_to_commit, Arc::new(JobRegistry::new()))),
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

    pub fn worker_cleanup_avg_sleep(mut self, d: Duration) -> Self {
        self.worker_cleanup_avg_sleep = Some(d);
        self
    }

    pub fn write_buffer(mut self, write_buffer: Arc<dyn WriteBufferWriting>) -> Self {
        self.write_buffer = Some(write_buffer);
        self
    }

    pub fn catalog_transactions_until_checkpoint(mut self, interval: NonZeroU64) -> Self {
        self.catalog_transactions_until_checkpoint = Some(interval);
        self
    }

    pub fn partition_template(mut self, template: PartitionTemplate) -> Self {
        self.partition_template = Some(template);
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
