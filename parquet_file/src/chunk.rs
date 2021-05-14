use snafu::{OptionExt, ResultExt, Snafu};
use std::{collections::BTreeSet, sync::Arc};

use crate::table::Table;
use data_types::{partition_metadata::TableSummary, timestamp::TimestampRange};
use datafusion::physical_plan::SendableRecordBatchStream;
use internal_types::{schema::Schema, selection::Selection};
use object_store::{path::Path, ObjectStore};
use query::predicate::Predicate;

use metrics::GaugeValue;
use std::mem;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error writing table '{}': {}", table_name, source))]
    TableWrite {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display("Table Error in '{}': {}", table_name, source))]
    NamedTableError {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display("Table '{}' not found in chunk {}", table_name, chunk_id))]
    NamedTableNotFoundInChunk { table_name: String, chunk_id: u64 },

    #[snafu(display(
        "Error read parquet file for table '{}', chunk {}",
        table_name,
        chunk_id
    ))]
    ReadParquet {
        table_name: String,
        chunk_id: u64,
        source: crate::table::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct ChunkMetrics {
    /// keep track of memory used by chunk
    memory_bytes: GaugeValue,
}

impl ChunkMetrics {
    /// Creates an instance of ChunkMetrics that isn't registered with a central
    /// metrics registry. Observations made to instruments on this ChunkMetrics instance
    /// will therefore not be visible to other ChunkMetrics instances or metric instruments
    /// created on a metrics domain, and vice versa
    pub fn new_unregistered() -> Self {
        Self {
            memory_bytes: GaugeValue::new_unregistered(),
        }
    }

    pub fn new(_metrics: &metrics::Domain, memory_bytes: GaugeValue) -> Self {
        Self { memory_bytes }
    }
}

#[derive(Debug)]
pub struct Chunk {
    /// Partition this chunk belongs to
    partition_key: String,

    /// The id for this chunk
    id: u32,

    /// Tables of this chunk
    tables: Vec<Table>,

    metrics: ChunkMetrics,
}

impl Chunk {
    pub fn new(part_key: String, chunk_id: u32, metrics: ChunkMetrics) -> Self {
        let mut chunk = Self {
            partition_key: part_key,
            id: chunk_id,
            tables: Default::default(),
            metrics,
        };
        chunk.metrics.memory_bytes.set(chunk.size());
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
    pub fn add_table(
        &mut self,
        table_summary: TableSummary,
        file_location: Path,
        store: Arc<ObjectStore>,
        schema: Schema,
        range: Option<TimestampRange>,
    ) {
        self.tables.push(Table::new(
            table_summary,
            file_location,
            store,
            schema,
            range,
        ));
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

    /// Return Schema for the specified table / columns
    pub fn table_schema(&self, table_name: &str, selection: Selection<'_>) -> Result<Schema> {
        let table = self.find_table(table_name)?;

        table
            .schema(selection)
            .context(NamedTableError { table_name })
    }

    /// Return object store path for specified table
    pub fn table_path(&self, table_name: &str) -> Result<Path> {
        let table = self.find_table(table_name)?;
        Ok(table.path())
    }

    /// Return Schema for the specified table / columns
    pub fn timestamp_range(&self, table_name: &str) -> Result<Option<TimestampRange>> {
        let table = self.find_table(table_name)?;
        Ok(table.timestamp_range())
    }

    // Return all tables of this chunk whose timestamp overlaps with the give one
    pub fn table_names(
        &self,
        timestamp_range: Option<TimestampRange>,
    ) -> impl Iterator<Item = String> + '_ {
        self.tables.iter().flat_map(move |t| {
            if t.matches_predicate(&timestamp_range) {
                Some(t.name())
            } else {
                None
            }
        })
    }

    // Return columns names of a given table that belong to the given column
    // selection
    pub fn column_names(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Option<BTreeSet<String>> {
        let table = self.find_table(table_name);

        match table {
            Ok(table) => table.column_names(selection),
            Err(_) => None,
        }
    }

    /// Return stream of data read from parquet file of the given table
    pub fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream> {
        let table = self.find_table(table_name)?;

        table
            .read_filter(predicate, selection)
            .context(ReadParquet {
                table_name,
                chunk_id: self.id(),
            })
    }

    /// The total number of rows in all row groups in all tables in this chunk.
    pub fn rows(&self) -> usize {
        self.tables.iter().map(|t| t.rows()).sum()
    }

    fn find_table(&self, table_name: &str) -> Result<&Table> {
        self.tables
            .iter()
            .find(|t| t.has_table(table_name))
            .context(NamedTableNotFoundInChunk {
                table_name,
                chunk_id: self.id(),
            })
    }
}
