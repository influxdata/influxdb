use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::{
    array::DictionaryArray,
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use snafu::{ensure, ResultExt, Snafu};

use data_types::partition_metadata::TableSummary;
use data_types::timestamp::TimestampRange;
use data_types::{
    error::ErrorLogger,
    partition_metadata::{ColumnSummary, Statistics},
};
use internal_types::schema::{Schema, TIME_COLUMN_NAME};
use internal_types::selection::Selection;

use super::Chunk;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Table not found: {}", table_name))]
    TableNotFound { table_name: String },

    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns {
        source: internal_types::schema::Error,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A queryable snapshot of a mutable buffer chunk
#[derive(Debug)]
pub struct ChunkSnapshot {
    schema: Arc<Schema>,
    batch: RecordBatch,
    table_name: Arc<str>,
    stats: Vec<ColumnSummary>,
    memory: metrics::GaugeValue,
}

impl ChunkSnapshot {
    pub(crate) fn new(chunk: &Chunk, memory: metrics::GaugeValue) -> Self {
        let table = &chunk.table;

        let schema = table
            .schema(Selection::All)
            .log_if_error("ChunkSnapshot getting table schema")
            .unwrap();

        let batch = table
            .to_arrow(Selection::All)
            .log_if_error("ChunkSnapshot converting table to arrow")
            .unwrap();

        let mut s = Self {
            schema: Arc::new(schema),
            batch,
            table_name: Arc::clone(&chunk.table_name),
            stats: table.stats(),
            memory,
        };
        s.memory.set(s.size());
        s
    }

    /// returns true if there is no data in this snapshot
    pub fn is_empty(&self) -> bool {
        self.batch.num_rows() == 0
    }

    /// Return true if this snapshot has the specified table name
    pub fn has_table(&self, table_name: &str) -> bool {
        self.table_name.as_ref() == table_name
    }

    /// Return Schema for the specified table / columns
    pub fn table_schema(&self, table_name: &str, selection: Selection<'_>) -> Result<Schema> {
        // Temporary #1295
        ensure!(
            self.table_name.as_ref() == table_name,
            TableNotFound { table_name }
        );

        Ok(match selection {
            Selection::All => self.schema.as_ref().clone(),
            Selection::Some(columns) => {
                let columns = self.schema.select(columns).context(SelectColumns)?;
                self.schema.project(&columns)
            }
        })
    }

    /// Return Schema for all columns in this snapshot
    pub fn full_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Returns a list of tables with writes matching the given timestamp_range
    pub fn table_names(&self, timestamp_range: Option<TimestampRange>) -> BTreeSet<String> {
        let mut ret = BTreeSet::new();
        if self.matches_predicate(&timestamp_range) {
            ret.insert(self.table_name.to_string());
        }
        ret
    }

    /// Returns a RecordBatch with the given selection
    pub fn read_filter(&self, table_name: &str, selection: Selection<'_>) -> Result<RecordBatch> {
        // Temporary #1295
        ensure!(
            self.table_name.as_ref() == table_name,
            TableNotFound { table_name }
        );

        Ok(match selection {
            Selection::All => self.batch.clone(),
            Selection::Some(columns) => {
                let projection = self.schema.select(columns).context(SelectColumns)?;
                let schema = self.schema.project(&projection).into();
                let columns = projection
                    .into_iter()
                    .map(|x| Arc::clone(self.batch.column(x)))
                    .collect();

                RecordBatch::try_new(schema, columns).expect("failed to project record batch")
            }
        })
    }

    /// Returns a given selection of column names from a table
    pub fn column_names(
        &self,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Option<BTreeSet<String>> {
        // Temporary #1295
        if self.table_name.as_ref() != table_name {
            return None;
        }

        let fields = self.schema.inner().fields().iter();

        Some(match selection {
            Selection::Some(cols) => fields
                .filter_map(|x| {
                    if cols.contains(&x.name().as_str()) {
                        Some(x.name().clone())
                    } else {
                        None
                    }
                })
                .collect(),
            Selection::All => fields.map(|x| x.name().clone()).collect(),
        })
    }

    /// Returns a vec of the summary statistics of the tables in this chunk
    pub fn table_summary(&self) -> TableSummary {
        TableSummary {
            name: self.table_name.to_string(),
            columns: self.stats.clone(),
        }
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, statistics and their rows.
    pub fn size(&self) -> usize {
        let columns = self.column_sizes().map(|(_, size)| size).sum::<usize>();
        let stats = self.stats.iter().map(|c| c.size()).sum::<usize>();
        columns + stats + std::mem::size_of::<Self>()
    }

    /// Returns the number of bytes taken up by the shared dictionary
    pub fn dictionary_size(&self) -> usize {
        self.batch
            .columns()
            .iter()
            .filter_map(|array| {
                let dict = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()?;
                let values = dict.values();
                Some(values.get_buffer_memory_size() + values.get_array_memory_size())
            })
            .next()
            .unwrap_or(0)
    }

    /// Returns an iterator over (column_name, estimated_size) for all
    /// columns in this chunk.
    ///
    /// Dictionary-encoded columns do not include the size of the shared dictionary
    /// in their reported total
    ///
    /// This is instead returned as a special "__dictionary" column
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        let dictionary_size = self.dictionary_size();
        self.batch
            .columns()
            .iter()
            .zip(self.stats.iter())
            .map(move |(array, summary)| {
                let size = match array.data_type() {
                    // Dictionary is only encoded once for all columns
                    DataType::Dictionary(_, _) => {
                        array.get_array_memory_size() + array.get_buffer_memory_size()
                            - dictionary_size
                    }
                    _ => array.get_array_memory_size() + array.get_buffer_memory_size(),
                };
                (summary.name.as_str(), size)
            })
            .chain(std::iter::once(("__dictionary", dictionary_size)))
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.batch.num_rows()
    }

    fn matches_predicate(&self, timestamp_range: &Option<TimestampRange>) -> bool {
        let timestamp_range = match timestamp_range {
            Some(t) => t,
            None => return true,
        };

        self.schema
            .find_index_of(TIME_COLUMN_NAME)
            .and_then(|idx| match &self.stats[idx].stats {
                Statistics::I64(stats) => Some(
                    !TimestampRange::new(stats.min? as _, stats.max? as _)
                        .disjoint(timestamp_range),
                ),
                _ => panic!("invalid statistics for time column"),
            })
            .unwrap_or(false) // If no time column or no time column values - cannot match
    }
}
