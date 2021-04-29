use data_types::{
    chunk::{ChunkStorage, ChunkSummary},
    database_rules::DatabaseRules,
    server_id::ServerId,
    DatabaseName,
};
use object_store::{disk::File, ObjectStore};
use query::{exec::Executor, Database};

use crate::{db::Db, JobRegistry};
use std::{convert::TryFrom, sync::Arc};
use tempfile::TempDir;

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
    // TODO: When we support parquet file in memory, we will either turn this test back to memory
    // or have both tests: local disk and memory
    //let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
    //
    // Create an object store with a specified location in a local disk
    let root = TempDir::new().unwrap();
    let object_store = Arc::new(ObjectStore::new_file(File::new(root.path())));
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
pub fn make_database(server_id: ServerId, object_store: Arc<ObjectStore>, db_name: &str) -> TestDb {
    let exec = Arc::new(Executor::new(1));
    let metrics_registry = Arc::new(metrics::MetricRegistry::new());
    TestDb {
        metric_registry: metrics::TestMetricRegistry::new(Arc::clone(&metrics_registry)),
        db: Db::new(
            DatabaseRules::new(DatabaseName::new(db_name.to_string()).unwrap()),
            server_id,
            object_store,
            exec,
            None, // write buffer
            Arc::new(JobRegistry::new()),
            metrics_registry,
        ),
    }
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
