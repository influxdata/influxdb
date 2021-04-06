use data_types::{
    chunk::{ChunkStorage, ChunkSummary},
    database_rules::DatabaseRules,
    DatabaseName,
};
use query::Database;

use crate::{db::Db, JobRegistry};
use std::sync::Arc;

/// Used for testing: create a Database with a local store
pub fn make_db() -> Db {
    Db::new(
        DatabaseRules::new(DatabaseName::new("placeholder").unwrap()),
        read_buffer::Database::new(),
        None, // wal buffer
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
