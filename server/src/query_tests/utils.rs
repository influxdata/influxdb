use data_types::{
    chunk::{ChunkStorage, ChunkSummary},
    database_rules::DatabaseRules,
    DatabaseName,
};
use object_store::{memory::InMemory, ObjectStore};
use query::{exec::Executor, Database};

use crate::{db::Db, JobRegistry};
use std::{num::NonZeroU32, sync::Arc};

/// Used for testing: create a Database with a local store
pub fn make_db() -> Db {
    let server_id: NonZeroU32 = NonZeroU32::new(1).unwrap();
    let object_store = Arc::new(ObjectStore::new_in_memory(InMemory::new()));
    let exec = Arc::new(Executor::new(1));

    Db::new(
        DatabaseRules::new(DatabaseName::new("placeholder").unwrap()),
        server_id,
        object_store,
        exec,
        None, // write buffer
        Arc::new(JobRegistry::new()),
    )
}

pub fn make_database(server_id: NonZeroU32, object_store: Arc<ObjectStore>, db_name: &str) -> Db {
    let exec = Arc::new(Executor::new(1));
    Db::new(
        DatabaseRules::new(DatabaseName::new(db_name.to_string()).unwrap()),
        server_id,
        object_store,
        exec,
        None, // write buffer
        Arc::new(JobRegistry::new()),
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
