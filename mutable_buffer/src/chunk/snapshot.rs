use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use data_types::timestamp::TimestampRange;
use internal_types::schema::{Schema, TIME_COLUMN_NAME};
use internal_types::selection::Selection;
use snafu::{ensure, ResultExt, Snafu};

use super::Chunk;
use data_types::{error::ErrorLogger, partition_metadata::Statistics};

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
    schema: Schema,
    batch: RecordBatch,
    table_name: Arc<str>,
    timestamp_range: Option<TimestampRange>,
    // TODO: Memory tracking
}

impl ChunkSnapshot {
    pub fn new(chunk: &Chunk) -> Self {
        let table = &chunk.table;

        let schema = table
            .schema(&chunk.dictionary, Selection::All)
            .log_if_error("ChunkSnapshot getting table schema")
            .unwrap();

        let batch = table
            .to_arrow(&chunk.dictionary, Selection::All)
            .log_if_error("ChunkSnapshot converting table to arrow")
            .unwrap();

        let timestamp_range =
            chunk
                .dictionary
                .lookup_value(TIME_COLUMN_NAME)
                .and_then(|column_id| {
                    table
                        .column(column_id)
                        .ok()
                        .and_then(|column| match column.stats() {
                            Statistics::I64(stats) => match (stats.min, stats.max) {
                                (Some(min), Some(max)) => Some(TimestampRange::new(min, max)),
                                _ => None,
                            },
                            _ => None,
                        })
                });

        Self {
            schema,
            batch,
            table_name: Arc::clone(&chunk.table_name),
            timestamp_range,
        }
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
            Selection::All => self.schema.clone(),
            Selection::Some(columns) => {
                let columns = self.schema.select(columns).context(SelectColumns)?;
                self.schema.project(&columns)
            }
        })
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

    fn matches_predicate(&self, timestamp_range: &Option<TimestampRange>) -> bool {
        match (self.timestamp_range, timestamp_range) {
            (Some(a), Some(b)) => !a.disjoint(b),
            (None, Some(_)) => false, /* If this chunk doesn't have a time column it can't match */
            // the predicate
            (_, None) => true,
        }
    }
}
