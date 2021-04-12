use std::collections::BTreeSet;

use crate::table::Table;
use data_types::partition_metadata::TableSummary;
use object_store::path::Path;
use tracker::{MemRegistry, MemTracker};

use std::mem;

#[derive(Debug)]
pub struct Chunk {
    /// Partition this chunk belongs to
    partition_key: String,

    /// The id for this chunk
    id: u32,

    /// Tables of this chunk
    tables: Vec<Table>,

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

    /// Return the chunk id
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Return the chunk's partition key
    pub fn partition_key(&self) -> &str {
        self.partition_key.as_ref()
    }

    /// Return all paths of this chunks
    pub fn all_paths(&self) -> Vec<Path> {
        self.tables.iter().map(|t| t.path()).collect()
    }

    /// Returns a vec of the summary statistics of the tables in this chunk
    pub fn table_summaries(&self) -> Vec<TableSummary> {
        self.tables.iter().map(|t| t.table_summary()).collect()
    }

    /// Add a chunk's table and its summary
    pub fn add_table(&mut self, table_summary: TableSummary, file_location: Path) {
        self.tables.push(Table::new(table_summary, file_location));
    }

    /// Return true if this chunk includes the given table
    pub fn has_table(&self, table_name: &str) -> bool {
        self.tables.iter().any(|t| t.has_table(table_name))
    }

    // Return all tables of this chunk
    pub fn all_table_names(&self, names: &mut BTreeSet<String>) {
        let mut tables = self
            .tables
            .iter()
            .map(|t| t.name())
            .collect::<BTreeSet<String>>();

        names.append(&mut tables);
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        let size: usize = self.tables.iter().map(|t| t.size()).sum();

        size + self.partition_key.len() + mem::size_of::<u32>() + mem::size_of::<Self>()
    }
}
