//! Represents a Chunk of data (a collection of tables and their data within
//! some chunk) in the mutable store.
use std::{
    collections::{BTreeSet, HashMap},
    convert::TryFrom,
    sync::Arc,
};

use parking_lot::Mutex;
use snafu::{ResultExt, Snafu};

use arrow::record_batch::RecordBatch;
use data_types::{partition_metadata::TableSummary, server_id::ServerId};
use entry::{ClockValue, TableBatch};
use internal_types::selection::Selection;
use tracker::{MemRegistry, MemTracker};

use crate::chunk::snapshot::ChunkSnapshot;
use crate::dictionary::{Dictionary, DID};
use crate::table::Table;

pub mod snapshot;

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

    #[snafu(display("Table {} not found in chunk {}", table, chunk))]
    TableNotFoundInChunk { table: DID, chunk: u64 },

    #[snafu(display("Column ID {} not found in dictionary of chunk {}", column_id, chunk))]
    ColumnIdNotFoundInDictionary { column_id: DID, chunk: u64 },

    #[snafu(display(
        "Column name {} not found in dictionary of chunk {}",
        column_name,
        chunk_id
    ))]
    ColumnNameNotFoundInDictionary { column_name: String, chunk_id: u64 },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Chunk {
    /// The id for this chunk
    id: u32,

    /// `dictionary` maps &str -> DID. The DIDs are used in place of String or
    /// str to avoid slow string operations. The same dictionary is used for
    /// table names, tag names, tag values, and column names.
    dictionary: Dictionary,

    /// map of the dictionary ID for the table name to the table
    tables: HashMap<DID, Table>,

    /// keep track of memory used by chunk
    tracker: MemTracker,

    /// Cached chunk snapshot
    ///
    /// Note: This is a mutex to allow mutation within
    /// `Chunk::snapshot()` which only takes an immutable borrow
    snapshot: Mutex<Option<Arc<ChunkSnapshot>>>,
}

impl Chunk {
    pub fn new(id: u32, memory_registry: &MemRegistry) -> Self {
        let mut chunk = Self {
            id,
            dictionary: Dictionary::new(),
            tables: HashMap::new(),
            tracker: memory_registry.register(),
            snapshot: Mutex::new(None),
        };
        chunk.tracker.set_bytes(chunk.size());
        chunk
    }

    pub fn write_table_batches(
        &mut self,
        clock_value: ClockValue,
        server_id: ServerId,
        batches: &[TableBatch<'_>],
    ) -> Result<()> {
        for batch in batches {
            let table_name = batch.name();
            let table_id = self.dictionary.lookup_value_or_insert(table_name);

            let table = self
                .tables
                .entry(table_id)
                .or_insert_with(|| Table::new(table_id));

            let columns = batch.columns();
            table
                .write_columns(&mut self.dictionary, clock_value, server_id, columns)
                .context(TableWrite { table_name })?;
        }

        // Invalidate chunk snapshot
        *self
            .snapshot
            .try_lock()
            .expect("concurrent readers/writers to MBChunk") = None;

        self.tracker.set_bytes(self.size());

        Ok(())
    }

    // Add all tables names in this chunk to `names` if they are not already present
    pub fn all_table_names(&self, names: &mut BTreeSet<String>) {
        for &table_id in self.tables.keys() {
            let table_name = self.dictionary.lookup_id(table_id).unwrap();
            if !names.contains(table_name) {
                names.insert(table_name.to_string());
            }
        }
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(not(feature = "nocache"))]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        let mut guard = self.snapshot.lock();
        if let Some(snapshot) = &*guard {
            return Arc::clone(snapshot);
        }

        // TODO: Incremental snapshot generation
        let snapshot = Arc::new(ChunkSnapshot::new(self));
        *guard = Some(Arc::clone(&snapshot));
        snapshot
    }

    /// Returns a queryable snapshot of this chunk
    #[cfg(feature = "nocache")]
    pub fn snapshot(&self) -> Arc<ChunkSnapshot> {
        Arc::new(ChunkSnapshot::new(self))
    }

    /// returns true if there is no data in this chunk
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// return the ID of this chunk
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Convert the table specified in this chunk into some number of
    /// record batches, appended to dst
    pub fn table_to_arrow(
        &self,
        dst: &mut Vec<RecordBatch>,
        table_name: &str,
        selection: Selection<'_>,
    ) -> Result<()> {
        if let Some(table) = self.table(table_name) {
            dst.push(
                table
                    .to_arrow(&self.dictionary, selection)
                    .context(NamedTableError { table_name })?,
            );
        }
        Ok(())
    }

    /// Returns a vec of the summary statistics of the tables in this chunk
    pub fn table_summaries(&self) -> Vec<TableSummary> {
        self.tables
            .iter()
            .map(|(&table_id, table)| {
                let name = self
                    .dictionary
                    .lookup_id(table_id)
                    .expect("table name not found in dictionary");

                TableSummary {
                    name: name.to_string(),
                    columns: table.stats(&self.dictionary),
                }
            })
            .collect()
    }

    /// Returns the named table, or None if no such table exists in this chunk
    fn table(&self, table_name: &str) -> Option<&Table> {
        self.dictionary
            .lookup_value(table_name)
            .and_then(|value| self.tables.get(&value))
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        let data_size: usize = self.tables.values().map(|t| t.size()).sum();
        data_size + self.dictionary.size()
    }

    /// Returns an iterator over (column_name, estimated_size) for all
    /// columns in this chunk.
    ///
    /// NOTE: also returns a special "__dictionary" column with the size of
    /// the dictionary that is shared across all columns in this chunk
    pub fn column_sizes(&self) -> impl Iterator<Item = (&str, usize)> + '_ {
        self.tables
            .values()
            .flat_map(|t| t.column_sizes())
            .map(move |(did, sz)| {
                let column_name = self
                    .dictionary
                    .lookup_id(*did)
                    .expect("column name in dictionary");
                (column_name, sz)
            })
            .chain(std::iter::once(("__dictionary", self.dictionary.size())))
    }

    /// Return the number of rows in this chunk
    pub fn rows(&self) -> usize {
        self.tables.values().map(|t| t.row_count()).sum()
    }

    /// Return true if this chunk has the specified table name
    pub fn has_table(&self, table_name: &str) -> bool {
        self.table(table_name).is_some()
    }
}

pub mod test_helpers {
    use entry::test_helpers::lp_to_entry;

    use super::*;

    /// A helper that will write line protocol string to the passed in Chunk.
    /// All data will be under a single partition with a clock value and
    /// server id of 1.
    pub fn write_lp_to_chunk(lp: &str, chunk: &mut Chunk) -> Result<()> {
        let entry = lp_to_entry(lp);

        for w in entry.partition_writes().unwrap() {
            chunk.write_table_batches(
                ClockValue::try_from(5).unwrap(),
                ServerId::try_from(1).unwrap(),
                &w.table_batches(),
            )?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;

    use super::test_helpers::write_lp_to_chunk;

    #[test]
    fn writes_table_batches() {
        let mr = MemRegistry::new();
        let mut chunk = Chunk::new(1, &mr);

        let lp = vec![
            "cpu,host=a val=23 1",
            "cpu,host=b val=2 1",
            "mem,host=a val=23432i 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk, "cpu")
        );

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-------+",
                "| host | time                          | val   |",
                "+------+-------------------------------+-------+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23432 |",
                "+------+-------------------------------+-------+",
            ],
            &chunk_to_batches(&chunk, "mem")
        );
    }

    #[test]
    fn writes_table_3_batches() {
        let mr = MemRegistry::new();
        let mut chunk = Chunk::new(1, &mr);

        let lp = vec![
            "cpu,host=a val=23 1",
            "cpu,host=b val=2 1",
            "mem,host=a val=23432i 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        let lp = vec![
            "cpu,host=c val=11 1",
            "mem sval=\"hi\" 2",
            "disk val=true 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();

        assert_batches_eq!(
            vec![
                "+------+-------------------------------+-----+",
                "| host | time                          | val |",
                "+------+-------------------------------+-----+",
                "| a    | 1970-01-01 00:00:00.000000001 | 23  |",
                "| b    | 1970-01-01 00:00:00.000000001 | 2   |",
                "| c    | 1970-01-01 00:00:00.000000001 | 11  |",
                "+------+-------------------------------+-----+",
            ],
            &chunk_to_batches(&chunk, "cpu")
        );

        assert_batches_eq!(
            vec![
                "+-------------------------------+------+",
                "| time                          | val  |",
                "+-------------------------------+------+",
                "| 1970-01-01 00:00:00.000000001 | true |",
                "+-------------------------------+------+",
            ],
            &chunk_to_batches(&chunk, "disk")
        );

        assert_batches_eq!(
            vec![
                "+------+------+-------------------------------+-------+",
                "| host | sval | time                          | val   |",
                "+------+------+-------------------------------+-------+",
                "| a    |      | 1970-01-01 00:00:00.000000001 | 23432 |",
                "|      | hi   | 1970-01-01 00:00:00.000000002 |       |",
                "+------+------+-------------------------------+-------+",
            ],
            &chunk_to_batches(&chunk, "mem")
        );
    }

    #[test]
    fn test_snapshot() {
        let mr = MemRegistry::new();
        let mut chunk = Chunk::new(1, &mr);

        let lp = vec![
            "cpu,host=a val=23 1",
            "cpu,host=b val=2 1",
            "mem,host=a val=23432i 1",
        ]
        .join("\n");

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s1 = chunk.snapshot();
        let s2 = chunk.snapshot();

        write_lp_to_chunk(&lp, &mut chunk).unwrap();
        let s3 = chunk.snapshot();
        let s4 = chunk.snapshot();

        assert_eq!(Arc::as_ptr(&s1), Arc::as_ptr(&s2));
        assert_ne!(Arc::as_ptr(&s1), Arc::as_ptr(&s3));
        assert_eq!(Arc::as_ptr(&s3), Arc::as_ptr(&s4));
    }

    fn chunk_to_batches(chunk: &Chunk, table: &str) -> Vec<RecordBatch> {
        let mut batches = vec![];
        chunk
            .table_to_arrow(&mut batches, table, Selection::All)
            .unwrap();
        batches
    }
}
