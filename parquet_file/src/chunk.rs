use snafu::{ResultExt, Snafu};
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

    #[snafu(display("Table '{}' not found in chunk", table_name))]
    NamedTableNotFoundInChunk { table_name: String },

    #[snafu(display("Error read parquet file for table '{}'", table_name,))]
    ReadParquet {
        table_name: String,
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

    /// The table in chunk
    table: Table,

    metrics: ChunkMetrics,
}

impl Chunk {
    pub fn new(
        part_key: impl Into<String>,
        table_summary: TableSummary,
        file_location: Path,
        store: Arc<ObjectStore>,
        schema: Schema,
        metrics: ChunkMetrics,
    ) -> Self {
        let table = Table::new(table_summary, file_location, store, schema);

        let mut chunk = Self {
            partition_key: part_key.into(),
            table,
            metrics,
        };

        chunk.metrics.memory_bytes.set(chunk.size());
        chunk
    }

    /// Return the chunk's partition key
    pub fn partition_key(&self) -> &str {
        self.partition_key.as_ref()
    }

    /// Return object store path for this chunk
    pub fn table_path(&self) -> Path {
        self.table.path()
    }

    /// Returns the summary statistics for this chunk
    pub fn table_summary(&self) -> TableSummary {
        self.table.table_summary()
    }

    /// Returns the name of the table this chunk holds
    pub fn table_name(&self) -> &str {
        self.table.name()
    }

    /// Return true if this chunk includes the given table
    pub fn has_table(&self, table_name: &str) -> bool {
        self.table_name() == table_name
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        self.table.size() + self.partition_key.len() + mem::size_of::<Self>()
    }

    /// Return Schema for the table in this chunk
    pub fn table_schema(&self, selection: Selection<'_>) -> Result<Schema> {
        self.table.schema(selection).context(NamedTableError {
            table_name: self.table_name(),
        })
    }

    // Return all tables of this chunk whose timestamp overlaps with the give one
    pub fn table_names(
        &self,
        timestamp_range: Option<TimestampRange>,
    ) -> impl Iterator<Item = String> + '_ {
        std::iter::once(&self.table)
            .filter(move |table| table.matches_predicate(&timestamp_range))
            .map(|table| table.name().to_string())
    }

    // Return the columns names that belong to the given column
    // selection
    pub fn column_names(&self, selection: Selection<'_>) -> Option<BTreeSet<String>> {
        self.table.column_names(selection)
    }

    /// Return stream of data read from parquet file of the given table
    pub fn read_filter(
        &self,
        table_name: &str,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream> {
        self.table
            .read_filter(predicate, selection)
            .context(ReadParquet { table_name })
    }

    /// The total number of rows in all row groups in all tables in this chunk.
    pub fn rows(&self) -> usize {
        self.table.rows()
    }
}
