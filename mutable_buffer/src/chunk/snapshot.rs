use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use data_types::timestamp::TimestampRange;
use internal_types::schema::{Schema, TIME_COLUMN_NAME};
use internal_types::selection::Selection;
use snafu::{OptionExt, ResultExt, Snafu};

use super::Chunk;
use data_types::partition_metadata::Statistics;

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
    /// The ID of the chunk this is a snapshot of
    chunk_id: u32,

    /// Maps table name to `TableSnapshot`
    records: HashMap<String, TableSnapshot>,
    // TODO: Memory tracking
}

#[derive(Debug)]
struct TableSnapshot {
    schema: Schema,
    batch: RecordBatch,
    timestamp_range: Option<TimestampRange>,
}

impl TableSnapshot {
    fn matches_predicate(&self, timestamp_range: &Option<TimestampRange>) -> bool {
        match (self.timestamp_range, timestamp_range) {
            (Some(a), Some(b)) => !a.disjoint(b),
            (None, Some(_)) => false, /* If this chunk doesn't have a time column it can't match */
            // the predicate
            (_, None) => true,
        }
    }
}

impl ChunkSnapshot {
    pub fn new(chunk: &Chunk) -> Self {
        let mut records: HashMap<String, TableSnapshot> = Default::default();
        for (id, table) in &chunk.tables {
            let schema = table.schema(&chunk.dictionary, Selection::All).unwrap();
            let batch = table.to_arrow(&chunk.dictionary, Selection::All).unwrap();
            let name = chunk.dictionary.lookup_id(*id).unwrap();

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

            records.insert(
                name.to_string(),
                TableSnapshot {
                    schema,
                    batch,
                    timestamp_range,
                },
            );
        }

        Self {
            chunk_id: chunk.id,
            records,
        }
    }

    /// return the ID of the chunk this is a snapshot of
    pub fn chunk_id(&self) -> u32 {
        self.chunk_id
    }

    /// returns true if there is no data in this snapshot
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Return true if this snapshot has the specified table name
    pub fn has_table(&self, table_name: &str) -> bool {
        self.records.get(table_name).is_some()
    }

    /// Return Schema for the specified table / columns
    pub fn table_schema(&self, table_name: &str, selection: Selection<'_>) -> Result<Schema> {
        let table = self
            .records
            .get(table_name)
            .context(TableNotFound { table_name })?;

        Ok(match selection {
            Selection::All => table.schema.clone(),
            Selection::Some(columns) => {
                let columns = table.schema.select(columns).context(SelectColumns)?;
                table.schema.project(&columns)
            }
        })
    }

    /// Returns a list of tables with writes matching the given timestamp_range
    pub fn table_names(
        &self,
        timestamp_range: Option<TimestampRange>,
    ) -> impl Iterator<Item = &String> + '_ {
        self.records
            .iter()
            .flat_map(move |(table_name, table_snapshot)| {
                table_snapshot
                    .matches_predicate(&timestamp_range)
                    .then(|| table_name)
            })
    }

    /// Returns a RecordBatch with the given selection
    pub fn read_filter(&self, table_name: &str, selection: Selection<'_>) -> Result<RecordBatch> {
        let table = self
            .records
            .get(table_name)
            .context(TableNotFound { table_name })?;

        Ok(match selection {
            Selection::All => table.batch.clone(),
            Selection::Some(columns) => {
                let projection = table.schema.select(columns).context(SelectColumns)?;
                let schema = table.schema.project(&projection).into();
                let columns = projection
                    .into_iter()
                    .map(|x| Arc::clone(table.batch.column(x)))
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
        let table = self.records.get(table_name)?;
        let fields = table.schema.inner().fields().iter();

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
}
