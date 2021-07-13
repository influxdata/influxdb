use super::MBChunk;
use arrow::record_batch::RecordBatch;
use data_types::{
    error::ErrorLogger,
    partition_metadata::{ColumnSummary, Statistics, TableSummary},
    timestamp::TimestampRange,
};
use internal_types::{
    schema::{Schema, TIME_COLUMN_NAME},
    selection::Selection,
};
use snafu::{ResultExt, Snafu};
use std::{collections::BTreeSet, sync::Arc};

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
}

impl ChunkSnapshot {
    pub(crate) fn new(chunk: &MBChunk) -> Self {
        let schema = chunk
            .schema(Selection::All)
            .log_if_error("ChunkSnapshot getting table schema")
            .unwrap();

        let batch = chunk
            .to_arrow(Selection::All)
            .log_if_error("ChunkSnapshot converting table to arrow")
            .unwrap();

        let summary = chunk.table_summary();

        Self {
            schema: Arc::new(schema),
            batch,
            table_name: Arc::clone(&chunk.table_name),
            stats: summary.columns,
        }
    }

    /// Return Schema for all columns in this snapshot
    pub fn full_schema(&self) -> Arc<Schema> {
        Arc::clone(&self.schema)
    }

    /// Returns a RecordBatch with the given selection
    pub fn read_filter(&self, selection: Selection<'_>) -> Result<RecordBatch> {
        Ok(match selection {
            Selection::All => self.batch.clone(),
            Selection::Some(columns) => {
                let projection = self
                    .schema
                    .compute_select_indicies(columns)
                    .context(SelectColumns)?;
                let schema = self.schema.select_by_indices(&projection).into();
                let columns = projection
                    .into_iter()
                    .map(|x| Arc::clone(self.batch.column(x)))
                    .collect();

                RecordBatch::try_new(schema, columns).expect("failed to project record batch")
            }
        })
    }

    /// Returns a given selection of column names from a table
    pub fn column_names(&self, selection: Selection<'_>) -> Option<BTreeSet<String>> {
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

    /// Returns an iterator over (column_name, estimated_size) for all
    /// columns in this chunk.
    ///
    /// This is instead returned as a special "__dictionary" column
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.batch
            .columns()
            .iter()
            .zip(self.stats.iter())
            .map(move |(array, summary)| {
                let size = array.get_array_memory_size() + array.get_buffer_memory_size();
                (summary.name.as_str(), size)
            })
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.batch.num_rows()
    }

    pub fn has_timerange(&self, timestamp_range: &Option<TimestampRange>) -> bool {
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
