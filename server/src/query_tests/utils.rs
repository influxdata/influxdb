use data_types::database_rules::DatabaseRules;
use mutable_buffer::MutableBufferDb;

use crate::db::Db;

/// Used for testing: create a Database with a local store
pub fn make_db() -> Db {
    let name = "test_db";
    Db::new(
        DatabaseRules::default(),
        Some(MutableBufferDb::new(name)),
        read_buffer::Database::new(),
        None, // wal buffer
    )
}
