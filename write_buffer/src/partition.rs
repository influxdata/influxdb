use arrow_deps::{
    arrow::record_batch::RecordBatch,
    datafusion::{
        logical_plan::Expr, logical_plan::Operator, optimizer::utils::expr_to_column_names,
        prelude::*,
    },
};
use generated_types::wal as wb;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use wal::{Entry as WalEntry, Result as WalResult};

use data_types::TIME_COLUMN_NAME;
use query::{
    predicate::{Predicate, TimestampRange},
    util::{visit_expression, AndExprBuilder, ExpressionVisitor},
};

use crate::dictionary::Dictionary;
use crate::table::Table;

use snafu::{OptionExt, ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Could not read WAL entry: {}", source))]
    WalEntryRead { source: wal::Error },

    #[snafu(display("Partition {} not found", partition))]
    PartitionNotFound { partition: String },

    #[snafu(display(
        "Column name {} not found in dictionary of partition {}",
        column,
        partition
    ))]
    ColumnNameNotFoundInDictionary {
        column: String,
        partition: String,
        source: crate::dictionary::Error,
    },

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

    #[snafu(display(
        "Table name {} not found in dictionary of partition {}",
        table,
        partition
    ))]
    TableNameNotFoundInDictionary {
        table: String,
        partition: String,
        source: crate::dictionary::Error,
    },

    #[snafu(display("Table {} not found in partition {}", table, partition))]
    TableNotFoundInPartition { table: u32, partition: String },

    #[snafu(display("Attempt to write table batch without a name"))]
    TableWriteWithoutName,

    #[snafu(display("Error restoring WAL entry, missing partition key"))]
    MissingPartitionKey,
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
pub struct Partition {
    pub key: String,

    /// `dictionary` maps &str -> u32. The u32s are used in place of String or str to avoid slow
    /// string operations. The same dictionary is used for table names, tag names, tag values, and
    /// column names.
    // TODO: intern string field values too?
    pub dictionary: Dictionary,

    /// map of the dictionary ID for the table name to the table
    pub tables: HashMap<u32, Table>,

    pub is_open: bool,
}

/// Describes the result of translating a set of strings into
/// partition specific ids
#[derive(Debug, PartialEq, Eq)]
pub enum PartitionIdSet {
    /// At least one of the strings was not present in the partitions'
    /// dictionary.
    ///
    /// This is important when testing for the presence of all ids in
    /// a set, as we know they can not all be present
    AtLeastOneMissing,

    /// All strings existed in this partition's dictionary
    Present(BTreeSet<u32>),
}

/// a 'Compiled' set of predicates / filters that can be evaluated on
/// this partition (where strings have been translated to partition
/// specific u32 ids)
#[derive(Debug)]
pub struct PartitionPredicate {
    /// If present, restrict the request to just those tables whose
    /// names are in table_names. If present but empty, means there
    /// was a predicate but no tables named that way exist in the
    /// partition (so no table can pass)
    pub table_name_predicate: Option<BTreeSet<u32>>,

    // Optional field column selection. If present, further restrict
    // any field columns returnedto only those named
    pub field_restriction: Option<BTreeSet<u32>>,

    /// General DataFusion expressions (arbitrary predicates) applied
    /// as a filter using logical conjuction (aka are 'AND'ed
    /// together). Only rows that evaluate to TRUE for all these
    /// expressions should be returned.
    pub partition_exprs: Vec<Expr>,

    /// If Some, then the table must contain all columns specified
    /// to pass the predicate
    pub required_columns: Option<PartitionIdSet>,

    /// The id of the "time" column in this partition
    pub time_column_id: u32,

    /// Timestamp range: only rows within this range should be considered
    pub range: Option<TimestampRange>,
}

impl PartitionPredicate {
    /// Creates and adds a datafuson predicate representing the
    /// combination of predicate and timestamp.
    pub fn filter_expr(&self) -> Option<Expr> {
        // build up a list of expressions
        let mut builder =
            AndExprBuilder::default().append_opt(self.make_timestamp_predicate_expr());

        for expr in &self.partition_exprs {
            builder = builder.append_expr(expr.clone());
        }

        builder.build()
    }

    /// Return true if there is a non empty field restriction
    pub fn has_field_restriction(&self) -> bool {
        match &self.field_restriction {
            None => false,
            Some(field_restiction) => !field_restiction.is_empty(),
        }
    }

    /// For plans which select a subset of fields, returns true if
    /// the field should be included in the results
    pub fn should_include_field(&self, field_id: u32) -> bool {
        match &self.field_restriction {
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

impl Partition {
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            dictionary: Dictionary::new(),
            tables: HashMap::new(),
            is_open: true,
        }
    }

    pub fn write_entry(&mut self, entry: &wb::WriteBufferEntry<'_>) -> Result<()> {
        if let Some(table_batches) = entry.table_batches() {
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

    /// Translates `predicate` into per-partition ids that can be
    /// directly evaluated against tables in this partition
    pub fn compile_predicate(&self, predicate: &Predicate) -> Result<PartitionPredicate> {
        let table_name_predicate = self.compile_string_list(predicate.table_names.as_ref());

        let field_restriction = self.compile_string_list(predicate.field_columns.as_ref());

        let time_column_id = self
            .dictionary
            .lookup_value(TIME_COLUMN_NAME)
            .expect("time is in the partition dictionary");

        let range = predicate.range;

        // it would be nice to avoid cloning all the exprs here.
        let partition_exprs = predicate.exprs.clone();

        // In order to evaluate expressions in the table, all columns
        // referenced in the expression must appear (I think, not sure
        // about NOT, etc so panic if we see one of those);
        let mut visitor = SupportVisitor {};
        let mut predicate_columns: HashSet<String> = HashSet::new();
        for expr in &partition_exprs {
            visit_expression(expr, &mut visitor);
            expr_to_column_names(&expr, &mut predicate_columns).unwrap();
        }

        // if there are any column references in the expression, ensure they appear in any table
        let required_columns = if predicate_columns.is_empty() {
            None
        } else {
            Some(self.make_partition_ids(predicate_columns.iter()))
        };

        Ok(PartitionPredicate {
            table_name_predicate,
            field_restriction,
            partition_exprs,
            required_columns,
            time_column_id,
            range,
        })
    }

    /// Converts a potential set of strings into a set of ids in terms
    /// of this dictionary. If there are no matching Strings in the
    /// partitions dictionary, those strings are ignored and a
    /// (potentially empty) set is returned.
    fn compile_string_list(&self, names: Option<&BTreeSet<String>>) -> Option<BTreeSet<u32>> {
        names.map(|names| {
            names
                .iter()
                .filter_map(|name| self.dictionary.id(name))
                .collect::<BTreeSet<_>>()
        })
    }

    /// Adds the ids of any columns in additional_required_columns to the required columns of predicate
    pub fn add_required_columns_to_predicate(
        &self,
        additional_required_columns: &HashSet<String>,
        predicate: &mut PartitionPredicate,
    ) {
        for column_name in additional_required_columns {
            // Once know we have missing columns, no need to try
            // and figure out if these any additional columns are needed
            if Some(PartitionIdSet::AtLeastOneMissing) == predicate.required_columns {
                return;
            }

            let column_id = self.dictionary.id(column_name);

            // Update the required colunm list
            predicate.required_columns = Some(match predicate.required_columns.take() {
                None => {
                    if let Some(column_id) = column_id {
                        let mut symbols = BTreeSet::new();
                        symbols.insert(column_id);
                        PartitionIdSet::Present(symbols)
                    } else {
                        PartitionIdSet::AtLeastOneMissing
                    }
                }
                Some(PartitionIdSet::Present(mut symbols)) => {
                    if let Some(column_id) = column_id {
                        symbols.insert(column_id);
                        PartitionIdSet::Present(symbols)
                    } else {
                        PartitionIdSet::AtLeastOneMissing
                    }
                }
                Some(PartitionIdSet::AtLeastOneMissing) => {
                    unreachable!("Covered by case above while adding required columns to predicate")
                }
            });
        }
    }

    /// returns true if data with partition key `key` should be
    /// written to this partition,
    pub fn should_write(&self, key: &str) -> bool {
        self.key.starts_with(key) && self.is_open
    }

    /// Convert the table specified in this partition into an arrow record batch
    pub fn table_to_arrow(&self, table_name: &str, columns: &[&str]) -> Result<RecordBatch> {
        let table_id =
            self.dictionary
                .lookup_value(table_name)
                .context(TableNameNotFoundInDictionary {
                    table: table_name,
                    partition: &self.key,
                })?;

        let table = self
            .tables
            .get(&table_id)
            .context(TableNotFoundInPartition {
                table: table_id,
                partition: &self.key,
            })?;
        table
            .to_arrow(&self, columns)
            .context(NamedTableError { table_name })
    }

    /// Translate a bunch of strings into a set of ids relative to this partition
    pub fn make_partition_ids<'a, I>(&self, predicate_columns: I) -> PartitionIdSet
    where
        I: Iterator<Item = &'a String>,
    {
        let mut symbols = BTreeSet::new();
        for column_name in predicate_columns {
            if let Some(column_id) = self.dictionary.id(column_name) {
                symbols.insert(column_id);
            } else {
                return PartitionIdSet::AtLeastOneMissing;
            }
        }

        PartitionIdSet::Present(symbols)
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
                "Unsupported expression in write_buffer database: {:?}",
                expr
            ),
        }
    }
}

#[derive(Default, Debug)]
pub struct RestorationStats {
    pub row_count: usize,
    pub tables: BTreeSet<String>,
}

/// Given a set of WAL entries, restore them into a set of Partitions.
pub fn restore_partitions_from_wal(
    wal_entries: impl Iterator<Item = WalResult<WalEntry>>,
) -> Result<(Vec<Partition>, RestorationStats)> {
    let mut stats = RestorationStats::default();

    let mut partitions = BTreeMap::new();

    for wal_entry in wal_entries {
        let wal_entry = wal_entry.context(WalEntryRead)?;
        let bytes = wal_entry.as_data();

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&bytes);

        if let Some(entries) = batch.entries() {
            for entry in entries {
                let partition_key = entry.partition_key().context(MissingPartitionKey)?;

                if !partitions.contains_key(partition_key) {
                    partitions.insert(
                        partition_key.to_string(),
                        Partition::new(partition_key.to_string()),
                    );
                }

                let partition = partitions
                    .get_mut(partition_key)
                    .context(PartitionNotFound {
                        partition: partition_key,
                    })?;

                partition.write_entry(&entry)?;
            }
        }
    }
    let partitions = partitions
        .into_iter()
        .map(|(_, p)| p)
        .collect::<Vec<Partition>>();

    // compute the stats
    for p in &partitions {
        for (id, table) in &p.tables {
            let name = p
                .dictionary
                .lookup_id(*id)
                .expect("table id wasn't inserted into dictionary on restore");
            if !stats.tables.contains(name) {
                stats.tables.insert(name.to_string());
            }

            stats.row_count += table.row_count();
        }
    }

    Ok((partitions, stats))
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
