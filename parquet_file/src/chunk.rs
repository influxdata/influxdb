use std::collections::BTreeSet;

use crate::table::Table;
use data_types::partition_metadata::TableSummary;
use object_store::path::Path;
use tracker::{MemRegistry, MemTracker};

#[derive(Debug)]
pub struct Chunk {
    /// Partition this chunk belongs to
    pub partition_key: String,

    /// The id for this chunk
    pub id: u32,

    /// Tables of this chunk
    pub tables: Vec<Table>,

    /// Track memory used by this chunk
    memory_tracker: MemTracker,
}

impl Chunk {
    pub fn new(part_key: String, chunk_id: u32, memory_registry: &MemRegistry) -> Self {
        let mut chunk = Self {
            partition_key: part_key,
            id: chunk_id,
            tables: Default::default(),
            memory_tracker: memory_registry.register(),
        };
        chunk.memory_tracker.set_bytes(chunk.size());
        chunk
    }

    pub fn add_table(&mut self, table_summary: TableSummary, file_location: Path) {
        self.tables.push(Table::new(table_summary, file_location));
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        // TODO: check if this table exists in the chunk
        if table_name.is_empty() {
            return false;
        }
        true
    }

    pub fn all_table_names(&self, names: &mut BTreeSet<String>) {
        // TODO
        names.insert("todo".to_string());
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        // TODO
        0
    }
}
