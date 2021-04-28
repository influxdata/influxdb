use data_types::{
    chunk::{ChunkStorage, ChunkSummary},
    database_rules::DatabaseRules,
    server_id::ServerId,
    DatabaseName,
};
use object_store::{memory::InMemory, ObjectStore};
use query::{exec::Executor, Database};

use crate::{db::Db, JobRegistry};
use std::{convert::TryFrom, sync::Arc};

// A wrapper around a Db and a metrics registry allowing for isolated testing
// of a Db and its metrics.
#[derive(Debug)]
pub struct TestDb {
    pub db: Db,
    pub metric_registry: metrics::TestMetricRegistry,
}

/// Used for testing: create a Database with a local store
pub fn make_db() -> TestDb {
    let server_id = ServerId::try_from(1).unwrap();
    let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
    let exec = Arc::new(Executor::new(1));
    let metrics_registry = Arc::new(metrics::MetricRegistry::new());

    TestDb {
        metric_registry: metrics::TestMetricRegistry::new(Arc::clone(&metrics_registry)),
        db: Db::new(
            DatabaseRules::new(DatabaseName::new("placeholder").unwrap()),
            server_id,
            object_store,
            exec,
            None, // write buffer
            Arc::new(JobRegistry::new()),
            metrics_registry,
        ),
    }
}

/// Used for testing: create a Database with a local store and a specified name
pub fn make_database(server_id: ServerId, object_store: Arc<ObjectStore>, db_name: &str) -> Db {
    let exec = Arc::new(Executor::new(1));
    let metrics_registry = Arc::new(metrics::MetricRegistry::new());
    Db::new(
        DatabaseRules::new(DatabaseName::new(db_name.to_string()).unwrap()),
        server_id,
        object_store,
        exec,
        None, // write buffer
        Arc::new(JobRegistry::new()),
        metrics_registry,
    )
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
        .filter(|s| s.storage == ChunkStorage::ReadBuffer)
        .count()
}
