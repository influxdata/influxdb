//! Represents a Chunk of data (a collection of tables and their data within
//! some chunk) in the mutable store.
use arrow_deps::{arrow::record_batch::RecordBatch, datafusion::logical_plan::Expr};

use generated_types::wal as wb;
use std::collections::{BTreeSet, HashMap};

use data_types::partition_metadata::TableSummary;
use internal_types::{schema::Schema, selection::Selection};

use crate::{
    column::Column,
    dictionary::{Dictionary, Error as DictionaryError},
    pred::{ChunkPredicate, ChunkPredicateBuilder},
    table::Table,
};
use snafu::{OptionExt, ResultExt, Snafu};
use tracker::{MemRegistry, MemTracker};

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

    #[snafu(display("Error checking predicate in table {}: {}", table_id, source))]
    PredicateCheck {
        table_id: u32,
        source: crate::table::Error,
    },

    #[snafu(display("Error checking predicate in table '{}': {}", table_name, source))]
    NamedTablePredicateCheck {
        table_name: String,
        source: crate::table::Error,
    },

    #[snafu(display(
        "Unsupported predicate when mutable buffer table names. Found a general expression: {:?}",
        exprs
    ))]
    PredicateNotYetSupported { exprs: Vec<Expr> },

    #[snafu(display("Table ID {} not found in dictionary of chunk {}", table_id, chunk))]
    TableIdNotFoundInDictionary {
        table_id: u32,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display(
        "Internal error: table {} not found in dictionary of chunk {}",
        table_name,
        chunk_id
    ))]
    InternalTableNotFoundInDictionary { table_name: String, chunk_id: u32 },

    #[snafu(display("Table {} not found in chunk {}", table, chunk))]
    TableNotFoundInChunk { table: u32, chunk: u64 },

    #[snafu(display("Table '{}' not found in chunk {}", table_name, chunk_id))]
    NamedTableNotFoundInChunk { table_name: String, chunk_id: u64 },

    #[snafu(display("Attempt to write table batch without a name"))]
    TableWriteWithoutName,

    #[snafu(display("Value ID {} not found in dictionary of chunk {}", value_id, chunk_id))]
    InternalColumnValueIdNotFoundInDictionary {
        value_id: u32,
        chunk_id: u64,
        source: DictionaryError,
    },

    #[snafu(display("Column ID {} not found in dictionary of chunk {}", column_id, chunk))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column name {} not found in dictionary of chunk {}",
        column_name,
        chunk_id
    ))]
    ColumnNameNotFoundInDictionary {
        column_name: String,
        chunk_id: u64,
        source: DictionaryError,
    },

    #[snafu(display(
        "Column '{}' is not a string tag column and thus can not list values",
        column_name
    ))]
    UnsupportedColumnTypeForListingValues { column_name: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Chunk {
    /// The id for this chunk
    pub id: u32,

    /// `dictionary` maps &str -> u32. The u32s are used in place of String or
    /// str to avoid slow string operations. The same dictionary is used for
    /// table names, tag names, tag values, and column names.
    // TODO: intern string field values too?
    pub dictionary: Dictionary,

    /// map of the dictionary ID for the table name to the table
    pub tables: HashMap<u32, Table>,

    /// keep track of memory used by chunk
    tracker: MemTracker,
}

impl Clone for Chunk {
    fn clone(&self) -> Self {
        // TODO: The performance of this is not great - (#635)
        let mut ret = Self {
            id: self.id,
            dictionary: self.dictionary.clone(),
            tables: self.tables.clone(),
            tracker: self.tracker.clone_empty(),
        };

        ret.tracker.set_bytes(ret.size());
        ret
    }
}

impl Chunk {
    pub fn new(id: u32, memory_registry: &MemRegistry) -> Self {
        let mut chunk = Self {
            id,
            dictionary: Dictionary::new(),
            tables: HashMap::new(),
            tracker: memory_registry.register(),
        };
        chunk.tracker.set_bytes(chunk.size());
        chunk
    }

    pub fn write_entry(&mut self, entry: &wb::WriteBufferEntry<'_>) -> Result<()> {
        if let Some(table_batches) = entry.table_batches() {
            for batch in table_batches {
                self.write_table_batch(&batch)?;
            }
        }

        self.tracker.set_bytes(self.size());

        Ok(())
    }

    fn write_table_batch(&mut self, batch: &wb::TableWriteBatch<'_>) -> Result<()> {
        let table_name = batch.name().context(TableWriteWithoutName)?;
        let table_id = self.dictionary.lookup_value_or_insert(table_name);

        let table = self
            .tables
            .entry(table_id)
            .or_insert_with(|| Table::new(table_id));

        if let Some(rows) = batch.rows() {
            table
                .append_rows(&mut self.dictionary, &rows)
                .context(TableWrite { table_name })?;
        }

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

    /// Return all the names of the tables names in this chunk that match
    /// chunk predicate
    pub fn table_names(&self, chunk_predicate: &ChunkPredicate) -> Result<Vec<&str>> {
        // we don't support arbitrary expressions in chunk predicate yet
        if !chunk_predicate.chunk_exprs.is_empty() {
            return PredicateNotYetSupported {
                exprs: chunk_predicate.chunk_exprs.clone(),
            }
            .fail();
        }

        self.tables
            .iter()
            .filter_map(|(&table_id, table)| {
                // could match is good enough for this metadata query
                match table.could_match_predicate(chunk_predicate) {
                    Ok(true) => Some(self.dictionary.lookup_id(table_id).context(
                        TableIdNotFoundInDictionary {
                            table_id,
                            chunk: self.id,
                        },
                    )),
                    Ok(false) => None,
                    Err(e) => Some(Err(e).context(PredicateCheck { table_id })),
                }
            })
            .collect()
    }

    /// If the column names that match the predicate can be found
    /// from the predicate entirely using metadata, return those
    /// strings.
    ///
    /// If the predicate cannot be evaluated entirely with
    /// metadata, return `Ok(None)`.
    pub fn column_names(
        &self,
        table_name: &str,
        chunk_predicate: &ChunkPredicate,
        selection: Selection<'_>,
    ) -> Result<Option<BTreeSet<String>>> {
        // No support for general purpose expressions
        if !chunk_predicate.chunk_exprs.is_empty() {
            return Ok(None);
        }

        let table_name_id = self.table_name_id(table_name)?;

        let mut chunk_column_ids = BTreeSet::new();

        // Is this table in the chunk?
        if let Some(table) = self.tables.get(&table_name_id) {
            for (&column_id, column) in &table.columns {
                let column_matches_predicate = table
                    .column_matches_predicate(&column, chunk_predicate)
                    .context(NamedTableError { table_name })?;

                if column_matches_predicate {
                    chunk_column_ids.insert(column_id);
                }
            }
        }

        // Only return subset of these selection_cols if not all_cols
        let mut all_cols = true;
        let selection_cols = match selection {
            Selection::All => &[""],
            Selection::Some(cols) => {
                all_cols = false;
                cols
            }
        };

        let mut column_names = BTreeSet::new();
        for &column_id in &chunk_column_ids {
            let column_name =
                self.dictionary
                    .lookup_id(column_id)
                    .context(ColumnIdNotFoundInDictionary {
                        column_id,
                        chunk: self.id,
                    })?;

            if !column_names.contains(column_name)
                && (all_cols || selection_cols.contains(&column_name))
            {
                // only use columns in selection_cols
                column_names.insert(column_name.to_string());
            }
        }

        Ok(Some(column_names))
    }

    /// Return the id of the table in the chunk's dictionary
    fn table_name_id(&self, table_name: &str) -> Result<u32> {
        self.dictionary
            .id(table_name)
            .context(InternalTableNotFoundInDictionary {
                table_name,
                chunk_id: self.id(),
            })
    }

    /// Returns the strings of the specified Tag column that satisfy
    /// the predicate, if they can be determined entirely using metadata.
    ///
    /// If the predicate cannot be evaluated entirely with metadata,
    /// return `Ok(None)`.
    pub fn tag_column_values(
        &self,
        table_name: &str,
        column_name: &str,
        chunk_predicate: &ChunkPredicate,
    ) -> Result<Option<BTreeSet<String>>> {
        // No support for general purpose expressions
        if !chunk_predicate.chunk_exprs.is_empty() {
            return Ok(None);
        }
        let chunk_id = self.id();

        let table_name_id = self.table_name_id(table_name)?;

        // Is this table even in the chunk?
        let table = self
            .tables
            .get(&table_name_id)
            .context(NamedTableNotFoundInChunk {
                table_name,
                chunk_id,
            })?;

        // See if we can rule out the table entire on metadata
        let could_match = table
            .could_match_predicate(chunk_predicate)
            .context(NamedTablePredicateCheck { table_name })?;

        if !could_match {
            // No columns could match, return empty set
            return Ok(Default::default());
        }

        let column_id =
            self.dictionary
                .lookup_value(column_name)
                .context(ColumnNameNotFoundInDictionary {
                    column_name,
                    chunk_id,
                })?;

        let column = table
            .column(column_id)
            .context(NamedTableError { table_name })?;

        if let Column::Tag(column, _) = column {
            // if we have a timestamp predicate, find all values
            // where the timestamp is within range. Otherwise take
            // all values.

            // Collect matching ids into BTreeSet to deduplicate on
            // ids *before* looking up Strings
            let column_value_ids: BTreeSet<u32> = match chunk_predicate.range {
                None => {
                    // take all non-null values
                    column.iter().filter_map(|&s| s).collect()
                }
                Some(range) => {
                    // filter out all values that don't match the timestmap
                    let time_column = table
                        .column_i64(chunk_predicate.time_column_id)
                        .context(NamedTableError { table_name })?;

                    column
                        .iter()
                        .zip(time_column.iter())
                        .filter_map(|(&column_value_id, &timestamp_value)| {
                            if range.contains_opt(timestamp_value) {
                                column_value_id
                            } else {
                                None
                            }
                        })
                        .collect()
                }
            };

            // convert all the (deduplicated) ids to Strings
            let column_values = column_value_ids
                .into_iter()
                .map(|value_id| {
                    let value = self.dictionary.lookup_id(value_id).context(
                        InternalColumnValueIdNotFoundInDictionary { value_id, chunk_id },
                    )?;
                    Ok(value.to_string())
                })
                .collect::<Result<BTreeSet<String>>>()?;

            Ok(Some(column_values))
        } else {
            UnsupportedColumnTypeForListingValues { column_name }.fail()
        }
    }

    /// Return a builder suitable to create predicates for this Chunk
    pub fn predicate_builder(&self) -> Result<ChunkPredicateBuilder<'_>, crate::pred::Error> {
        ChunkPredicateBuilder::new(&self.dictionary)
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
        if let Some(table) = self.table(table_name)? {
            dst.push(
                table
                    .to_arrow(&self, selection)
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
                    columns: table.stats(&self),
                }
            })
            .collect()
    }

    /// Returns the named table, or None if no such table exists in this chunk
    fn table(&self, table_name: &str) -> Result<Option<&Table>> {
        let table_id = self.dictionary.lookup_value(table_name);

        let table = match table_id {
            Ok(table_id) => Some(self.tables.get(&table_id).context(TableNotFoundInChunk {
                table: table_id,
                chunk: self.id,
            })?),
            Err(_) => None,
        };
        Ok(table)
    }

    /// Return Schema for the specified table / columns
    pub fn table_schema(&self, table_name: &str, selection: Selection<'_>) -> Result<Schema> {
        let table = self
            .table(table_name)?
            // Option --> Result
            .context(NamedTableNotFoundInChunk {
                table_name,
                chunk_id: self.id(),
            })?;

        table
            .schema(self, selection)
            .context(NamedTableError { table_name })
    }

    /// Return the approximate memory size of the chunk, in bytes including the
    /// dictionary, tables, and their rows.
    pub fn size(&self) -> usize {
        let data_size = self.tables.values().fold(0, |acc, val| acc + val.size());
        data_size + self.dictionary.size
    }

    /// Return true if this chunk has the specified table name
    pub fn has_table(&self, table_name: &str) -> bool {
        matches!(self.table(table_name), Ok(Some(_)))
    }
}
