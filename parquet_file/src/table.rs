use snafu::{ResultExt, Snafu};
use std::mem;

use data_types::{partition_metadata::TableSummary, timestamp::TimestampRange};
use internal_types::{schema::Schema, selection::Selection};
use object_store::path::Path;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to select columns: {}", source))]
    SelectColumns {
        source: internal_types::schema::Error,
    },
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

    /// Schema that goes with this table's parquet file
    table_schema: Schema,

    /// Timestamp rang of this table's parquet file
    timestamp_range: Option<TimestampRange>,
}

impl Table {
    pub fn new(
        meta: TableSummary,
        path: Path,
        schema: Schema,
        range: Option<TimestampRange>,
    ) -> Self {
        Self {
            table_summary: meta,
            object_store_path: path,
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

    /// return schema of this table for specified selection columns
    pub fn schema(&self, selection: Selection<'_>) -> Result<Schema> {
        Ok(match selection {
            Selection::All => self.table_schema.clone(),
            Selection::Some(columns) => {
                let columns = self.table_schema.select(columns).context(SelectColumns)?;
                self.table_schema.project(&columns)
            }
        })
    }

    pub fn matches_predicate(&self, timestamp_range: &Option<TimestampRange>) -> bool {
        match (self.timestamp_range, timestamp_range) {
            (Some(a), Some(b)) => !a.disjoint(b),
            (None, Some(_)) => false, /* If this chunk doesn't have a time column it can't match */
            // the predicate
            (_, None) => true,
        }
    }
}
