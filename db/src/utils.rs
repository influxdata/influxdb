use crate::{load::load_or_create_preserved_catalog, DatabaseToCommit, Db};
use data_types::{
    chunk_metadata::ChunkSummary,
    database_rules::{DatabaseRules, LifecycleRules, PartitionTemplate, TemplatePart},
    server_id::ServerId,
    DatabaseName,
};
use iox_object_store::IoxObjectStore;
use iox_time::{Time, TimeProvider};
use job_registry::JobRegistry;
use object_store::{DynObjectStore, ObjectStoreImpl};
use persistence_windows::checkpoint::ReplayPlan;
use query::exec::Executor;
use query::exec::ExecutorConfig;
use std::{borrow::Cow, convert::TryFrom, num::NonZeroU32, sync::Arc, time::Duration};
use uuid::Uuid;

// A wrapper around a Db and a metric registry allowing for isolated testing
// of a Db and its metrics.
#[derive(Debug)]
pub struct TestDb {
    pub db: Arc<Db>,
    pub metric_registry: Arc<metric::Registry>,
    pub replay_plan: ReplayPlan,
}

impl TestDb {
    pub fn builder() -> TestDbBuilder {
        TestDbBuilder::new()
    }
}

#[derive(Debug)]
pub struct TestDbBuilder {
    server_id: ServerId,
    object_store: Arc<DynObjectStore>,
    db_name: DatabaseName<'static>,
    uuid: Uuid,
    worker_cleanup_avg_sleep: Duration,
    lifecycle_rules: LifecycleRules,
    partition_template: PartitionTemplate,
    time_provider: Arc<dyn TimeProvider>,
}

impl Default for TestDbBuilder {
    fn default() -> Self {
        Self {
            server_id: ServerId::try_from(1).unwrap(),
            object_store: Arc::new(ObjectStoreImpl::new_in_memory()),
            db_name: DatabaseName::new("placeholder").unwrap(),
            uuid: Uuid::new_v4(),
            // make background loop spin a bit faster for tests
            worker_cleanup_avg_sleep: Duration::from_secs(1),
            // default to quick lifecycle rules for faster tests
            lifecycle_rules: LifecycleRules {
                late_arrive_window_seconds: NonZeroU32::try_from(1).unwrap(),
                ..Default::default()
            },
            // default to hourly
            partition_template: PartitionTemplate {
                parts: vec![TemplatePart::TimeFormat("%Y-%m-%dT%H".to_string())],
            },
            time_provider: Arc::new(iox_time::SystemProvider::new()),
        }
    }
}

impl TestDbBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn build(&self) -> TestDb {
        let server_id = self.server_id;
        let object_store = Arc::clone(&self.object_store);
        let db_name = self.db_name.clone();
        let uuid = self.uuid;

        let time_provider = Arc::clone(&self.time_provider);

        let iox_object_store = IoxObjectStore::load(Arc::clone(&object_store), uuid).await;
        let iox_object_store = match iox_object_store {
            Ok(ios) => ios,
            Err(_) => IoxObjectStore::create(Arc::clone(&object_store), uuid)
                .await
                .unwrap(),
        };
        let iox_object_store = Arc::new(iox_object_store);

        // deterministic thread and concurrency count
        let exec = Arc::new(Executor::new_with_config(ExecutorConfig {
            num_threads: 1,
            target_query_partitions: 4,
        }));

        let metric_registry = Arc::new(metric::Registry::new());

        let (preserved_catalog, catalog, replay_plan) = load_or_create_preserved_catalog(
            db_name.as_str(),
            Arc::clone(&iox_object_store),
            Arc::clone(&metric_registry),
            Arc::clone(&time_provider),
            false,
            false,
        )
        .await
        .unwrap();

        let mut rules = DatabaseRules::new(db_name);
        rules.worker_cleanup_avg_sleep = self.worker_cleanup_avg_sleep;
        rules.lifecycle_rules = self.lifecycle_rules.clone();
        rules.partition_template = self.partition_template.clone();

        let jobs = Arc::new(JobRegistry::new(
            Default::default(),
            Arc::clone(&time_provider),
        ));

        let database_to_commit = DatabaseToCommit {
            rules: Arc::new(rules),
            server_id,
            iox_object_store,
            preserved_catalog,
            catalog,
            exec,
            metric_registry: Arc::clone(&metric_registry),
            time_provider,
        };

        TestDb {
            metric_registry,
            db: Arc::new(Db::new(database_to_commit, jobs)),
            replay_plan: replay_plan.expect("did not skip replay"),
        }
    }

    pub fn server_id(mut self, server_id: ServerId) -> Self {
        self.server_id = server_id;
        self
    }

    pub fn object_store(mut self, object_store: Arc<DynObjectStore>) -> Self {
        self.object_store = object_store;
        self
    }

    pub fn db_name<T: Into<Cow<'static, str>>>(mut self, db_name: T) -> Self {
        self.db_name = DatabaseName::new(db_name).unwrap();
        self
    }

    pub fn worker_cleanup_avg_sleep(mut self, d: Duration) -> Self {
        self.worker_cleanup_avg_sleep = d;
        self
    }

    pub fn lifecycle_rules(mut self, lifecycle_rules: LifecycleRules) -> Self {
        self.lifecycle_rules = lifecycle_rules;
        self
    }

    pub fn partition_template(mut self, template: PartitionTemplate) -> Self {
        self.partition_template = template;
        self
    }

    pub fn time_provider(mut self, time_provider: Arc<dyn TimeProvider>) -> Self {
        self.time_provider = time_provider;
        self
    }
}

/// Used for testing: create a Database with a local store
pub async fn make_db() -> TestDb {
    TestDb::builder().build().await
}

pub async fn make_db_time() -> (Arc<Db>, Arc<iox_time::MockProvider>) {
    let provider = Arc::new(iox_time::MockProvider::new(Time::from_timestamp(295293, 3)));
    let db = TestDb::builder()
        .time_provider(Arc::<iox_time::MockProvider>::clone(&provider))
        .build()
        .await
        .db;
    (db, provider)
}

/// Counts the number of chunks passing a predicate
fn count_table_chunks(
    db: &Db,
    table_name: Option<&str>,
    partition_key: Option<&str>,
    predicate: impl Fn(&ChunkSummary) -> bool,
) -> usize {
    db.filtered_chunk_summaries(table_name, partition_key)
        .into_iter()
        .filter(predicate)
        .count()
}

/// Returns the number of mutable buffer chunks in the specified database
pub fn count_mutable_buffer_chunks(db: &Db) -> usize {
    count_table_chunks(db, None, None, |s| s.storage.has_mutable_buffer())
}

/// return number of MUB chunks of a given table of a partition
pub fn count_mub_table_chunks(db: &Db, table_name: &str, partition_key: &str) -> usize {
    count_table_chunks(db, Some(table_name), Some(partition_key), |s| {
        s.storage.has_mutable_buffer()
    })
}

/// Returns the number of read buffer chunks in the specified database
pub fn count_read_buffer_chunks(db: &Db) -> usize {
    count_table_chunks(db, None, None, |s| s.storage.has_read_buffer())
}

/// return number of RUB chunks of a given table of a partition
pub fn count_rub_table_chunks(db: &Db, table_name: &str, partition_key: &str) -> usize {
    count_table_chunks(db, Some(table_name), Some(partition_key), |s| {
        s.storage.has_read_buffer()
    })
}

/// Returns the number of object store chunks in the specified database
pub fn count_object_store_chunks(db: &Db) -> usize {
    count_table_chunks(db, None, None, |s| s.storage.has_object_store())
}

/// return number of OS chunks of a given table of a partition
pub fn count_os_table_chunks(db: &Db, table_name: &str, partition_key: &str) -> usize {
    count_table_chunks(db, Some(table_name), Some(partition_key), |s| {
        s.storage.has_object_store()
    })
}

static PANIC_DATABASE: once_cell::race::OnceBox<parking_lot::Mutex<hashbrown::HashSet<String>>> =
    once_cell::race::OnceBox::new();

/// Register a key to trigger a panic when provided in a call to panic_test
pub fn register_panic_key(key: impl Into<String>) {
    let mut panic_database = PANIC_DATABASE.get_or_init(Default::default).lock();
    panic_database.insert(key.into());
}

/// Clear a key to trigger a panic when provided in a call to panic_test
pub fn clear_panic_key(key: impl AsRef<str>) {
    let mut panic_database = PANIC_DATABASE.get_or_init(Default::default).lock();
    panic_database.remove(key.as_ref());
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
