use snafu::{ResultExt, Snafu};
use std::{collections::BTreeSet, mem, sync::Arc};

use crate::storage::{self, Storage};
use data_types::{partition_metadata::TableSummary, timestamp::TimestampRange};
use datafusion::physical_plan::SendableRecordBatchStream;
use internal_types::{schema::Schema, selection::Selection};
use object_store::{path::Path, ObjectStore};
use query::predicate::Predicate;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns {
        source: internal_types::schema::Error,
    },

    #[snafu(display("Failed to read parquet: {}", source))]
    ReadParquet { source: storage::Error },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Table that belongs to a chunk persisted in a parquet file in object store
#[derive(Debug, Clone)]
pub struct Table {
    /// Meta data of the table
    table_summary: TableSummary,

    /// Path in the object store. Format:
    ///  <writer id>/<database>/data/<partition key>/<chunk
    /// id>/<tablename>.parquet
    object_store_path: Path,

    /// Object store of the above relative path to open and read the file
    object_store: Arc<ObjectStore>,

    /// Schema that goes with this table's parquet file
    table_schema: Schema,

    /// Timestamp rang of this table's parquet file
    timestamp_range: Option<TimestampRange>,
}

impl Table {
    pub fn new(
        meta: TableSummary,
        path: Path,
        store: Arc<ObjectStore>,
        schema: Schema,
        range: Option<TimestampRange>,
    ) -> Self {
        Self {
            table_summary: meta,
            object_store_path: path,
            object_store: store,
            table_schema: schema,
            timestamp_range: range,
        }
    }

    pub fn table_summary(&self) -> TableSummary {
        self.table_summary.clone()
    }

    pub fn has_table(&self, table_name: &str) -> bool {
        self.table_summary.has_table(table_name)
    }

    /// Return the approximate memory size of the table
    pub fn size(&self) -> usize {
        mem::size_of::<Self>()
            + self.table_summary.size()
            + mem::size_of_val(&self.object_store_path)
            + mem::size_of_val(&self.table_schema)
    }

    /// Return name of this table
    pub fn name(&self) -> String {
        self.table_summary.name.clone()
    }

    /// Return the object store path of this table
    pub fn path(&self) -> Path {
        self.object_store_path.clone()
    }

    /// Return schema of this table for specified selection columns
    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {
        Ok(match selection {
            Selection::All => self.table_schema.clone(),
            Selection::Some(columns) => {
                let columns = self.table_schema.select(columns).context(SelectColumns)?;
                self.table_schema.project(&columns)
            }
        })
    }

    /// Return timestamp range of this table
    pub fn timestamp_range(&self) -> Option<TimestampRange> {
        self.timestamp_range
    }

    // Check if 2 time ranges overlap
    pub fn matches_predicate(&self, timestamp_range: &Option<TimestampRange>) -> bool {
        match (self.timestamp_range, timestamp_range) {
            (Some(a), Some(b)) => !a.disjoint(b),
            (None, Some(_)) => false, /* If this chunk doesn't have a time column it can't match */
            // the predicate
            (_, None) => true,
        }
    }

    // Return columns names of this table that belong to the given column selection
    pub fn column_names(&self, selection: Selection<'_>) -> Option<BTreeSet<String>> {
        let fields = self.table_schema.inner().fields().iter();

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

    /// Return stream of data read from parquet file for given predicate and
    /// column selection
    pub fn read_filter(
        &self,
        predicate: &Predicate,
        selection: Selection<'_>,
    ) -> Result<SendableRecordBatchStream> {
        Storage::read_filter(
            predicate,
            selection,
            Arc::clone(&self.table_schema.as_arrow()),
            self.object_store_path.clone(),
            Arc::clone(&self.object_store),
        )
        .context(ReadParquet)
    }

    /// The number of rows of this table
    pub fn rows(&self) -> usize {
        // All columns have the same rows, so return get row count of the first column
        self.table_summary.columns[0].count() as usize
    }
}
