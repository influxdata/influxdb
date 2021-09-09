use crate::{
    db::{load::load_or_create_preserved_catalog, DatabaseToCommit, Db},
    JobRegistry,
};
use data_types::{
    chunk_metadata::{ChunkStorage, ChunkSummary},
    database_rules::{DatabaseRules, LifecycleRules, PartitionTemplate, TemplatePart},
    server_id::ServerId,
    DatabaseName,
};
use iox_object_store::IoxObjectStore;
use object_store::ObjectStore;
use persistence_windows::checkpoint::ReplayPlan;
use query::exec::ExecutorConfig;
use query::{exec::Executor, QueryDatabase};
use std::{borrow::Cow, convert::TryFrom, num::NonZeroU32, sync::Arc, time::Duration};
use write_buffer::config::WriteBufferConfig;

// A wrapper around a Db and a metrics registry allowing for isolated testing
// of a Db and its metrics.
#[derive(Debug)]
pub struct TestDb {
    pub db: Arc<Db>,
    pub metric_registry: metrics::TestMetricRegistry,
    pub metrics_registry_v2: Arc<metric::Registry>,
    pub replay_plan: ReplayPlan,
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
    write_buffer: Option<WriteBufferConfig>,
    lifecycle_rules: Option<LifecycleRules>,
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
            .unwrap_or_else(|| Arc::new(ObjectStore::new_in_memory()));

        let iox_object_store =
            IoxObjectStore::find_existing(Arc::clone(&object_store), server_id, &db_name)
                .await
                .unwrap();

        let iox_object_store = match iox_object_store {
            Some(ios) => ios,
            None => IoxObjectStore::new(Arc::clone(&object_store), server_id, &db_name)
                .await
                .unwrap(),
        };

        let iox_object_store = Arc::new(iox_object_store);

        // deterministic thread and concurrency count
        let exec = Arc::new(Executor::new_with_config(ExecutorConfig {
            num_threads: 1,
            target_query_partitions: 4,
        }));

        let metric_registry = Arc::new(metrics::MetricRegistry::new());
        let metrics_registry_v2 = Arc::new(metric::Registry::new());

        let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
            db_name.as_str(),
            Arc::clone(&iox_object_store),
            server_id,
            Arc::clone(&metric_registry),
            Arc::clone(&metrics_registry_v2),
            false,
            false,
        )
        .await
        .unwrap();

        let mut rules = DatabaseRules::new(db_name);

        // make background loop spin a bit faster for tests
        rules.worker_cleanup_avg_sleep = self
            .worker_cleanup_avg_sleep
            .unwrap_or_else(|| Duration::from_secs(1));

        // default to quick lifecycle rules for faster tests
        rules.lifecycle_rules = self.lifecycle_rules.unwrap_or_else(|| LifecycleRules {
            late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
            ..Default::default()
        });

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
            rules: Arc::new(rules),
            server_id,
            iox_object_store,
            preserved_catalog,
            catalog,
            write_buffer: self.write_buffer,
            exec,
            metrics_registry_v2: Arc::clone(&metrics_registry_v2),
        };

        TestDb {
            metric_registry: metrics::TestMetricRegistry::new(metric_registry),
            metrics_registry_v2,
            db: Db::new(
                database_to_commit,
                Arc::new(JobRegistry::new(Default::default())),
            ),
            replay_plan: replay_plan.expect("did not skip replay"),
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

    pub fn write_buffer(mut self, write_buffer: WriteBufferConfig) -> Self {
        self.write_buffer = Some(write_buffer);
        self
    }

    pub fn lifecycle_rules(mut self, lifecycle_rules: LifecycleRules) -> Self {
        self.lifecycle_rules = Some(lifecycle_rules);
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

static PANIC_DATABASE: once_cell::race::OnceBox<parking_lot::Mutex<hashbrown::HashSet<String>>> =
    once_cell::race::OnceBox::new();

/// Register a key to trigger a panic when provided in a call to panic_test
pub fn register_panic_key(key: impl Into<String>) {
    let mut panic_database = PANIC_DATABASE.get_or_init(Default::default).lock();
    panic_database.insert(key.into());
}

#[inline]
/// Used to induce panics in tests
///
/// This needs to be public and not test-only to allow usage by other crates, unfortunately
/// this means it currently isn't stripped out of production builds
///
/// The runtime cost if no keys are registered is that of a single atomic load, and so
/// it is deemed not worth the feature flag plumbing necessary to strip it out
pub fn panic_test(key: impl FnOnce() -> Option<String>) {
    if let Some(panic_database) = PANIC_DATABASE.get() {
        if let Some(key) = key() {
            if panic_database.lock().contains(&key) {
                panic!("key {} registered in panic database", key)
            }
        }
    }
}
