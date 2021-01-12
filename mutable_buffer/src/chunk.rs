//! Represents a Chunk of data (a collection of tables and their data within
//! some chunk) in the mutable store.
use arrow_deps::{
    arrow::record_batch::RecordBatch,
    datafusion::{
        logical_plan::Expr, logical_plan::Operator, optimizer::utils::expr_to_column_names,
        prelude::*,
    },
};
use chrono::{DateTime, Utc};
use generated_types::wal as wb;
use std::collections::{BTreeSet, HashMap, HashSet};

use data_types::{partition_metadata::Table as TableStats, TIME_COLUMN_NAME};
use query::{
    predicate::{Predicate, TimestampRange},
    util::{visit_expression, AndExprBuilder, ExpressionVisitor},
};

use crate::dictionary::{Dictionary, Error as DictionaryError};
use crate::table::Table;

use snafu::{OptionExt, ResultExt, Snafu};

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

    #[snafu(display("Table ID {} not found in dictionary of chunk {}", table, chunk))]
    TableIdNotFoundInDictionary {
        table: u32,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display("Table {} not found in chunk {}", table, chunk))]
    TableNotFoundInChunk { table: u32, chunk: u64 },

    #[snafu(display("Attempt to write table batch without a name"))]
    TableWriteWithoutName,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Chunk {
    /// The id for this chunk
    pub id: u32,

    /// Time at which the first data was written into this chunk. Note
    /// this is not the same as the timestamps on the data itself
    pub time_of_first_write: Option<DateTime<Utc>>,

    /// Most recent time at which data write was initiated into this
    /// chunk. Note this is not the same as the timestamps on the data
    /// itself
    pub time_of_last_write: Option<DateTime<Utc>>,

    /// Time at which this chunk was closed and became immutable (no
    /// new data was written after this time). Note this is not the
    /// same as the timestamps on the data itself
    pub time_closed: Option<DateTime<Utc>>,

    /// `dictionary` maps &str -> u32. The u32s are used in place of String or
    /// str to avoid slow string operations. The same dictionary is used for
    /// table names, tag names, tag values, and column names.
    // TODO: intern string field values too?
    pub dictionary: Dictionary,

    /// map of the dictionary ID for the table name to the table
    pub tables: HashMap<u32, Table>,
}

/// Describes the result of translating a set of strings into
/// chunk specific ids
#[derive(Debug, PartialEq, Eq)]
pub enum ChunkIdSet {
    /// At least one of the strings was not present in the chunks'
    /// dictionary.
    ///
    /// This is important when testing for the presence of all ids in
    /// a set, as we know they can not all be present
    AtLeastOneMissing,

    /// All strings existed in this chunk's dictionary
    Present(BTreeSet<u32>),
}

/// a 'Compiled' set of predicates / filters that can be evaluated on
/// this chunk (where strings have been translated to chunk
/// specific u32 ids)
#[derive(Debug)]
pub struct ChunkPredicate {
    /// If present, restrict the request to just those tables whose
    /// names are in table_names. If present but empty, means there
    /// was a predicate but no tables named that way exist in the
    /// chunk (so no table can pass)
    pub table_name_predicate: Option<BTreeSet<u32>>,

    /// Optional column restriction. If present, further
    /// restrict any field columns returned to only those named, and
    /// skip tables entirely when querying metadata that do not have
    /// *any* of the fields
    pub field_name_predicate: Option<BTreeSet<u32>>,

    /// General DataFusion expressions (arbitrary predicates) applied
    /// as a filter using logical conjuction (aka are 'AND'ed
    /// together). Only rows that evaluate to TRUE for all these
    /// expressions should be returned.
    pub chunk_exprs: Vec<Expr>,

    /// If Some, then the table must contain all columns specified
    /// to pass the predicate
    pub required_columns: Option<ChunkIdSet>,

    /// The id of the "time" column in this chunk
    pub time_column_id: u32,

    /// Timestamp range: only rows within this range should be considered
    pub range: Option<TimestampRange>,
}

impl ChunkPredicate {
    /// Creates and adds a datafuson predicate representing the
    /// combination of predicate and timestamp.
    pub fn filter_expr(&self) -> Option<Expr> {
        // build up a list of expressions
        let mut builder =
            AndExprBuilder::default().append_opt(self.make_timestamp_predicate_expr());

        for expr in &self.chunk_exprs {
            builder = builder.append_expr(expr.clone());
        }

        builder.build()
    }

    /// For plans which select a subset of fields, returns true if
    /// the field should be included in the results
    pub fn should_include_field(&self, field_id: u32) -> bool {
        match &self.field_name_predicate {
            None => true,
            Some(field_restriction) => field_restriction.contains(&field_id),
        }
    }

    /// Return true if this column is the time column
    pub fn is_time_column(&self, id: u32) -> bool {
        self.time_column_id == id
    }

    /// Creates a DataFusion predicate for appliying a timestamp range:
    ///
    /// range.start <= time and time < range.end`
    fn make_timestamp_predicate_expr(&self) -> Option<Expr> {
        self.range.map(|range| make_range_expr(&range))
    }
}

/// Creates expression like:
/// range.low <= time && time < range.high
fn make_range_expr(range: &TimestampRange) -> Expr {
    let ts_low = lit(range.start).lt_eq(col(TIME_COLUMN_NAME));
    let ts_high = col(TIME_COLUMN_NAME).lt(lit(range.end));

    ts_low.and(ts_high)
}

impl Chunk {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            dictionary: Dictionary::new(),
            tables: HashMap::new(),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        }
    }

    pub fn write_entry(&mut self, entry: &wb::WriteBufferEntry<'_>) -> Result<()> {
        if let Some(table_batches) = entry.table_batches() {
            let now = Utc::now();
            if self.time_of_first_write.is_none() {
                self.time_of_first_write = Some(now);
            }
            self.time_of_last_write = Some(now);

            for batch in table_batches {
                self.write_table_batch(&batch)?;
            }
        }

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

    /// Mark the chunk as closed
    pub fn mark_closed(&mut self) {
        assert!(self.time_closed.is_none());
        self.time_closed = Some(Utc::now())
    }

    /// Translates `predicate` into per-chunk ids that can be
    /// directly evaluated against tables in this chunk
    pub fn compile_predicate(&self, predicate: &Predicate) -> Result<ChunkPredicate> {
        let table_name_predicate = self.compile_string_list(predicate.table_names.as_ref());

        let field_restriction = self.compile_string_list(predicate.field_columns.as_ref());

        let time_column_id = self
            .dictionary
            .lookup_value(TIME_COLUMN_NAME)
            .expect("time is in the chunk dictionary");

        let range = predicate.range;

        // it would be nice to avoid cloning all the exprs here.
        let chunk_exprs = predicate.exprs.clone();

        // In order to evaluate expressions in the table, all columns
        // referenced in the expression must appear (I think, not sure
        // about NOT, etc so panic if we see one of those);
        let mut visitor = SupportVisitor {};
        let mut predicate_columns: HashSet<String> = HashSet::new();
        for expr in &chunk_exprs {
            visit_expression(expr, &mut visitor);
            expr_to_column_names(&expr, &mut predicate_columns).unwrap();
        }

        // if there are any column references in the expression, ensure they appear in
        // any table
        let required_columns = if predicate_columns.is_empty() {
            None
        } else {
            Some(self.make_chunk_ids(predicate_columns.iter()))
        };

        Ok(ChunkPredicate {
            table_name_predicate,
            field_name_predicate: field_restriction,
            chunk_exprs,
            required_columns,
            time_column_id,
            range,
        })
    }

    /// Converts a potential set of strings into a set of ids in terms
    /// of this dictionary. If there are no matching Strings in the
    /// chunks dictionary, those strings are ignored and a
    /// (potentially empty) set is returned.
    fn compile_string_list(&self, names: Option<&BTreeSet<String>>) -> Option<BTreeSet<u32>> {
        names.map(|names| {
            names
                .iter()
                .filter_map(|name| self.dictionary.id(name))
                .collect::<BTreeSet<_>>()
        })
    }

    /// Adds the ids of any columns in additional_required_columns to the
    /// required columns of predicate
    pub fn add_required_columns_to_predicate(
        &self,
        additional_required_columns: &HashSet<String>,
        predicate: &mut ChunkPredicate,
    ) {
        for column_name in additional_required_columns {
            // Once know we have missing columns, no need to try
            // and figure out if these any additional columns are needed
            if Some(ChunkIdSet::AtLeastOneMissing) == predicate.required_columns {
                return;
            }

            let column_id = self.dictionary.id(column_name);

            // Update the required colunm list
            predicate.required_columns = Some(match predicate.required_columns.take() {
                None => {
                    if let Some(column_id) = column_id {
                        let mut symbols = BTreeSet::new();
                        symbols.insert(column_id);
                        ChunkIdSet::Present(symbols)
                    } else {
                        ChunkIdSet::AtLeastOneMissing
                    }
                }
                Some(ChunkIdSet::Present(mut symbols)) => {
                    if let Some(column_id) = column_id {
                        symbols.insert(column_id);
                        ChunkIdSet::Present(symbols)
                    } else {
                        ChunkIdSet::AtLeastOneMissing
                    }
                }
                Some(ChunkIdSet::AtLeastOneMissing) => {
                    unreachable!("Covered by case above while adding required columns to predicate")
                }
            });
        }
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
        columns: &[&str],
    ) -> Result<()> {
        if let Some(table) = self.table(table_name)? {
            dst.push(
                table
                    .to_arrow(&self, columns)
                    .context(NamedTableError { table_name })?,
            );
        }
        Ok(())
    }

    /// Returns a vec of the summary statistics of the tables in this chunk
    pub fn table_stats(&self) -> Result<Vec<TableStats>> {
        let mut stats = Vec::with_capacity(self.tables.len());

        for (id, table) in &self.tables {
            let name = self
                .dictionary
                .lookup_id(*id)
                .context(TableIdNotFoundInDictionary {
                    table: *id,
                    chunk: self.id,
                })?;

            let columns = table.stats();

            stats.push(TableStats {
                name: name.to_string(),
                columns,
            });
        }

        Ok(stats)
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

    /// Translate a bunch of strings into a set of ids relative to this
    /// chunk
    pub fn make_chunk_ids<'a, I>(&self, predicate_columns: I) -> ChunkIdSet
    where
        I: Iterator<Item = &'a String>,
    {
        let mut symbols = BTreeSet::new();
        for column_name in predicate_columns {
            if let Some(column_id) = self.dictionary.id(column_name) {
                symbols.insert(column_id);
            } else {
                return ChunkIdSet::AtLeastOneMissing;
            }
        }

        ChunkIdSet::Present(symbols)
    }
}

impl query::PartitionChunk for Chunk {
    type Error = Error;

    fn id(&self) -> u32 {
        self.id
    }

    fn table_stats(&self) -> Result<Vec<TableStats>, Self::Error> {
        self.table_stats()
    }

    fn table_to_arrow(
        &self,
        dst: &mut Vec<RecordBatch>,
        table_name: &str,
        columns: &[&str],
    ) -> Result<(), Self::Error> {
        self.table_to_arrow(dst, table_name, columns)
    }
}

/// Used to figure out if we know how to deal with this kind of
/// predicate in the write buffer
struct SupportVisitor {}

impl ExpressionVisitor for SupportVisitor {
    fn pre_visit(&mut self, expr: &Expr) {
        match expr {
            Expr::Literal(..) => {}
            Expr::Column(..) => {}
            Expr::BinaryExpr { op, .. } => {
                match op {
                    Operator::Eq
                    | Operator::Lt
                    | Operator::LtEq
                    | Operator::Gt
                    | Operator::GtEq
                    | Operator::Plus
                    | Operator::Minus
                    | Operator::Multiply
                    | Operator::Divide
                    | Operator::And
                    | Operator::Or => {}
                    // Unsupported (need to think about ramifications)
                    Operator::NotEq | Operator::Modulus | Operator::Like | Operator::NotLike => {
                        panic!("Unsupported binary operator in expression: {:?}", expr)
                    }
                }
            }
            _ => panic!(
                "Unsupported expression in mutable_buffer database: {:?}",
                expr
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_range_expr() {
        // Test that the generated predicate is correct

        let range = TimestampRange::new(101, 202);

        let ts_predicate_expr = make_range_expr(&range);
        let expected_string = "Int64(101) LtEq #time And #time Lt Int64(202)";
        let actual_string = format!("{:?}", ts_predicate_expr);

        assert_eq!(actual_string, expected_string);
    }
}
