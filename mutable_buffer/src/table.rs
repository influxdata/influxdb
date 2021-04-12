use std::{
    cmp,
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
};

use crate::{
    chunk::Chunk,
    column,
    column::Column,
    dictionary::{Dictionary, Error as DictionaryError},
    pred::{ChunkIdSet, ChunkPredicate},
};
use data_types::{
    database_rules::WriterId,
    partition_metadata::{ColumnSummary, Statistics},
};
use internal_types::{
    entry::{self, ClockValue},
    schema::{builder::SchemaBuilder, Schema, TIME_COLUMN_NAME},
    selection::Selection,
};

use snafu::{OptionExt, ResultExt, Snafu};

use arrow_deps::{
    arrow,
    arrow::{
        array::{
            ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder, UInt64Builder,
        },
        datatypes::DataType as ArrowDataType,
        record_batch::RecordBatch,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Tag value ID {} not found in dictionary of chunk {}", value, chunk))]
    TagValueIdNotFoundInDictionary {
        value: u32,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display("Column error on column {}: {}", column, source))]
    ColumnError {
        column: String,
        source: column::Error,
    },

    #[snafu(display(
        "Internal error: Expected column {} to be type {} but was {}",
        column_id,
        expected_column_type,
        actual_column_type
    ))]
    InternalColumnTypeMismatch {
        column_id: u32,
        expected_column_type: String,
        actual_column_type: String,
    },

    #[snafu(display("Internal error: unexpected aggregate request for None aggregate",))]
    InternalUnexpectedNoneAggregate {},

    #[snafu(display(
        "Column name '{}' not found in dictionary of chunk {}",
        column_name,
        chunk
    ))]
    ColumnNameNotFoundInDictionary { column_name: String, chunk: u64 },

    #[snafu(display(
        "Internal: Column id '{}' not found in dictionary of chunk {}",
        column_id,
        chunk
    ))]
    ColumnIdNotFoundInDictionary {
        column_id: u32,
        chunk: u64,
        source: DictionaryError,
    },

    #[snafu(display("arrow conversion error: {}", source))]
    ArrowError { source: arrow::error::ArrowError },

    #[snafu(display("Internal error converting schema: {}", source))]
    InternalSchema {
        source: internal_types::schema::builder::Error,
    },

    #[snafu(display(
        "No index entry found for column {} with id {}",
        column_name,
        column_id
    ))]
    InternalNoColumnInIndex { column_name: String, column_id: u32 },

    #[snafu(display("Error creating column from wal for column {}: {}", column, source))]
    CreatingFromWal {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Error evaluating column predicate for column {}: {}", column, source))]
    ColumnPredicateEvaluation {
        column: u32,
        source: crate::column::Error,
    },

    #[snafu(display("Row insert to table {} missing column name", table))]
    ColumnNameNotInRow { table: u32 },

    #[snafu(display(
        "Group column '{}' not found in tag columns: {}",
        column_name,
        all_tag_column_names
    ))]
    GroupColumnNotFound {
        column_name: String,
        all_tag_column_names: String,
    },

    #[snafu(display("Duplicate group column '{}'", column_name))]
    DuplicateGroupColumn { column_name: String },

    #[snafu(display("Column {} not found in table {}", id, table_id))]
    ColumnIdNotFound { id: u32, table_id: u32 },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Table {
    /// Name of the table as a u32 in the chunk dictionary
    pub id: u32,

    /// Map of column id from the chunk dictionary to the column
    pub columns: BTreeMap<u32, Column>,
}

impl Table {
    pub fn new(id: u32) -> Self {
        Self {
            id,
            columns: BTreeMap::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.columns
            .values()
            .next()
            .map(|col| col.len())
            .unwrap_or(0)
    }

    /// The approximate memory size of the data in the table, in bytes. Note
    /// that the space taken for the tag string values is represented in the
    /// dictionary size in the chunk that holds the table.
    pub fn size(&self) -> usize {
        self.columns.values().fold(0, |acc, v| acc + v.size())
    }

    /// Returns a reference to the specified column
    pub(crate) fn column(&self, column_id: u32) -> Result<&Column> {
        self.columns.get(&column_id).context(ColumnIdNotFound {
            id: column_id,
            table_id: self.id,
        })
    }

    /// Returns a reference to the specified column as a slice of
    /// i64s. Errors if the type is not i64
    pub fn column_i64(&self, column_id: u32) -> Result<&[Option<i64>]> {
        let column = self.column(column_id)?;
        match column {
            Column::I64(vals, _) => Ok(vals),
            _ => InternalColumnTypeMismatch {
                column_id,
                expected_column_type: "i64",
                actual_column_type: column.type_description(),
            }
            .fail(),
        }
    }

    /// Validates the schema of the passed in columns, then adds their values to
    /// the associated columns in the table and updates summary statistics.
    pub fn write_columns(
        &mut self,
        dictionary: &mut Dictionary,
        _clock_value: ClockValue,
        _writer_id: WriterId,
        columns: Vec<entry::Column<'_>>,
    ) -> Result<()> {
        // get the column ids and validate schema for those that already exist
        let columns_with_inserts = columns
            .into_iter()
            .map(|insert_column| {
                let column_id = dictionary.lookup_value_or_insert(insert_column.name());
                let values = insert_column.values();

                if let Some(c) = self.columns.get(&column_id) {
                    match (&values, c) {
                        (entry::TypedValuesIterator::Bool(_), Column::Bool(_, _)) => (),
                        (entry::TypedValuesIterator::U64(_), Column::U64(_, _)) => (),
                        (entry::TypedValuesIterator::F64(_), Column::F64(_, _)) => (),
                        (entry::TypedValuesIterator::I64(_), Column::I64(_, _)) => (),
                        (entry::TypedValuesIterator::String(_), Column::String(_, _)) => {
                            if !insert_column.is_field() {
                                InternalColumnTypeMismatch {
                                    column_id,
                                    expected_column_type: c.type_description(),
                                    actual_column_type: values.type_description(),
                                }
                                .fail()?
                            };
                        }
                        (entry::TypedValuesIterator::String(_), Column::Tag(_, _)) => {
                            if !insert_column.is_tag() {
                                InternalColumnTypeMismatch {
                                    column_id,
                                    expected_column_type: c.type_description(),
                                    actual_column_type: values.type_description(),
                                }
                                .fail()?
                            };
                        }
                        _ => InternalColumnTypeMismatch {
                            column_id,
                            expected_column_type: c.type_description(),
                            actual_column_type: values.type_description(),
                        }
                        .fail()?,
                    }
                }

                Ok((column_id, insert_column.logical_type(), values))
            })
            .collect::<Result<Vec<_>>>()?;

        let row_count_before_insert = self.row_count();

        for (column_id, logical_type, values) in columns_with_inserts.into_iter() {
            match self.columns.get_mut(&column_id) {
                Some(c) => c
                    .push_typed_values(dictionary, logical_type, values)
                    .with_context(|| {
                        let column = dictionary
                            .lookup_id(column_id)
                            .expect("column name must be present in dictionary");
                        ColumnError { column }
                    })?,
                None => {
                    self.columns.insert(
                        column_id,
                        Column::new_from_typed_values(
                            dictionary,
                            row_count_before_insert,
                            logical_type,
                            values,
                        ),
                    );
                }
            }
        }

        // ensure all columns have the same number of rows as the one with the most.
        // This adds nulls to the columns that weren't included in this write
        let max_row_count = self
            .columns
            .values()
            .fold(row_count_before_insert, |max, col| cmp::max(max, col.len()));

        for c in self.columns.values_mut() {
            c.push_nulls_to_len(max_row_count);
        }

        Ok(())
    }

    /// Returns the column selection for all the columns in this table, orderd
    /// by table name
    fn all_columns_selection<'a>(&self, chunk: &'a Chunk) -> Result<TableColSelection<'a>> {
        let cols = self
            .columns
            .iter()
            .map(|(column_id, _)| {
                let column_name = chunk.dictionary.lookup_id(*column_id).context(
                    ColumnIdNotFoundInDictionary {
                        column_id: *column_id,
                        chunk: chunk.id,
                    },
                )?;
                Ok(ColSelection {
                    column_name,
                    column_id: *column_id,
                })
            })
            .collect::<Result<_>>()?;

        let selection = TableColSelection { cols };

        // sort so the columns always come out in a predictable name
        Ok(selection.sort_by_name())
    }

    /// Returns a column selection for just the specified columns
    fn specific_columns_selection<'a>(
        &self,
        chunk: &'a Chunk,
        columns: &'a [&'a str],
    ) -> Result<TableColSelection<'a>> {
        let cols =
            columns
                .iter()
                .map(|&column_name| {
                    let column_id = chunk.dictionary.id(column_name).context(
                        ColumnNameNotFoundInDictionary {
                            column_name,
                            chunk: chunk.id,
                        },
                    )?;

                    Ok(ColSelection {
                        column_name,
                        column_id,
                    })
                })
                .collect::<Result<_>>()?;

        Ok(TableColSelection { cols })
    }

    /// Converts this table to an arrow record batch.
    pub fn to_arrow(&self, chunk: &Chunk, selection: Selection<'_>) -> Result<RecordBatch> {
        // translate chunk selection into name/indexes:
        let selection = match selection {
            Selection::All => self.all_columns_selection(chunk),
            Selection::Some(cols) => self.specific_columns_selection(chunk, cols),
        }?;
        self.to_arrow_impl(chunk, &selection)
    }

    pub fn schema(&self, chunk: &Chunk, selection: Selection<'_>) -> Result<Schema> {
        // translate chunk selection into name/indexes:
        let selection = match selection {
            Selection::All => self.all_columns_selection(chunk),
            Selection::Some(cols) => self.specific_columns_selection(chunk, cols),
        }?;
        self.schema_impl(&selection)
    }

    /// Returns the Schema of this table
    fn schema_impl(&self, selection: &TableColSelection<'_>) -> Result<Schema> {
        let mut schema_builder = SchemaBuilder::new();

        for col in &selection.cols {
            let column_name = col.column_name;
            let column = self.column(col.column_id)?;

            schema_builder = match column {
                Column::String(_, _) => schema_builder.field(column_name, ArrowDataType::Utf8),
                Column::Tag(_, _) => schema_builder.tag(column_name),
                Column::F64(_, _) => schema_builder.field(column_name, ArrowDataType::Float64),
                Column::I64(_, _) => {
                    if column_name == TIME_COLUMN_NAME {
                        schema_builder.timestamp()
                    } else {
                        schema_builder.field(column_name, ArrowDataType::Int64)
                    }
                }
                Column::U64(_, _) => schema_builder.field(column_name, ArrowDataType::UInt64),
                Column::Bool(_, _) => schema_builder.field(column_name, ArrowDataType::Boolean),
            };
        }

        schema_builder.build().context(InternalSchema)
    }

    /// Converts this table to an arrow record batch,
    ///
    /// requested columns with index are tuples of column_name, column_index
    fn to_arrow_impl(
        &self,
        chunk: &Chunk,
        selection: &TableColSelection<'_>,
    ) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(selection.cols.len());

        for col in &selection.cols {
            let column = self.column(col.column_id)?;

            let array = match column {
                Column::String(vals, _) => {
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(s) => builder.append_value(s),
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish()) as ArrayRef
                }
                Column::Tag(vals, _) => {
                    let mut builder = StringBuilder::with_capacity(vals.len(), vals.len() * 10);

                    for v in vals {
                        match v {
                            None => builder.append_null(),
                            Some(value_id) => {
                                let tag_value = chunk.dictionary.lookup_id(*value_id).context(
                                    TagValueIdNotFoundInDictionary {
                                        value: *value_id,
                                        chunk: chunk.id,
                                    },
                                )?;
                                builder.append_value(tag_value)
                            }
                        }
                        .context(ArrowError {})?;
                    }

                    Arc::new(builder.finish()) as ArrayRef
                }
                Column::F64(vals, _) => {
                    let mut builder = Float64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish()) as ArrayRef
                }
                Column::I64(vals, _) => {
                    let mut builder = Int64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish()) as ArrayRef
                }
                Column::U64(vals, _) => {
                    let mut builder = UInt64Builder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish()) as ArrayRef
                }
                Column::Bool(vals, _) => {
                    let mut builder = BooleanBuilder::new(vals.len());

                    for v in vals {
                        builder.append_option(*v).context(ArrowError {})?;
                    }

                    Arc::new(builder.finish()) as ArrayRef
                }
            };

            columns.push(array);
        }

        let schema = self.schema_impl(selection)?.into();

        RecordBatch::try_new(schema, columns).context(ArrowError {})
    }

    /// returns true if any row in this table could possible match the
    /// predicate. true does not mean any rows will *actually* match,
    /// just that the entire table can not be ruled out.
    ///
    /// false means that no rows in this table could possibly match
    pub fn could_match_predicate(&self, chunk_predicate: &ChunkPredicate) -> Result<bool> {
        Ok(
            self.matches_column_name_predicate(chunk_predicate.field_name_predicate.as_ref())
                && self.matches_table_name_predicate(chunk_predicate.table_name_predicate.as_ref())
                && self.matches_timestamp_predicate(chunk_predicate)?
                && self.has_columns(chunk_predicate.required_columns.as_ref()),
        )
    }

    /// Returns true if the table contains any of the field columns
    /// requested or there are no specific fields requested.
    fn matches_column_name_predicate(&self, column_selection: Option<&BTreeSet<u32>>) -> bool {
        match column_selection {
            Some(column_selection) => {
                for column_id in column_selection {
                    if let Some(column) = self.columns.get(column_id) {
                        if !column.is_tag() {
                            return true;
                        }
                    }
                }

                // selection only had tag columns
                false
            }
            None => true, // no specific selection
        }
    }

    fn matches_table_name_predicate(&self, table_name_predicate: Option<&BTreeSet<u32>>) -> bool {
        match table_name_predicate {
            Some(table_name_predicate) => table_name_predicate.contains(&self.id),
            None => true, // no table predicate
        }
    }

    /// returns true if there are any timestamps in this table that
    /// fall within the timestamp range
    fn matches_timestamp_predicate(&self, chunk_predicate: &ChunkPredicate) -> Result<bool> {
        match &chunk_predicate.range {
            None => Ok(true),
            Some(range) => {
                let time_column_id = chunk_predicate.time_column_id;
                let time_column = self.column(time_column_id)?;
                time_column.has_i64_range(range.start, range.end).context(
                    ColumnPredicateEvaluation {
                        column: time_column_id,
                    },
                )
            }
        }
    }

    /// returns true if no columns are specified, or the table has all
    /// columns specified
    fn has_columns(&self, columns: Option<&ChunkIdSet>) -> bool {
        if let Some(columns) = columns {
            match columns {
                ChunkIdSet::AtLeastOneMissing => return false,
                ChunkIdSet::Present(symbols) => {
                    for symbol in symbols {
                        if !self.columns.contains_key(symbol) {
                            return false;
                        }
                    }
                }
            }
        }
        true
    }

    /// returns true if there are any rows in column that are non-null
    /// and within the timestamp range specified by pred
    pub(crate) fn column_matches_predicate(
        &self,
        column: &Column,
        chunk_predicate: &ChunkPredicate,
    ) -> Result<bool> {
        match column {
            Column::F64(v, _) => self.column_value_matches_predicate(v, chunk_predicate),
            Column::I64(v, _) => self.column_value_matches_predicate(v, chunk_predicate),
            Column::U64(v, _) => self.column_value_matches_predicate(v, chunk_predicate),
            Column::String(v, _) => self.column_value_matches_predicate(v, chunk_predicate),
            Column::Bool(v, _) => self.column_value_matches_predicate(v, chunk_predicate),
            Column::Tag(v, _) => self.column_value_matches_predicate(v, chunk_predicate),
        }
    }

    fn column_value_matches_predicate<T>(
        &self,
        column_value: &[Option<T>],
        chunk_predicate: &ChunkPredicate,
    ) -> Result<bool> {
        match chunk_predicate.range {
            None => Ok(true),
            Some(range) => {
                let time_column_id = chunk_predicate.time_column_id;
                let time_column = self.column(time_column_id)?;
                time_column
                    .has_non_null_i64_range(column_value, range.start, range.end)
                    .context(ColumnPredicateEvaluation {
                        column: time_column_id,
                    })
            }
        }
    }

    pub fn stats(&self, chunk: &Chunk) -> Vec<ColumnSummary> {
        self.columns
            .iter()
            .map(|(column_id, c)| {
                let column_name = chunk
                    .dictionary
                    .lookup_id(*column_id)
                    .expect("column name in dictionary");

                let stats = match c {
                    Column::F64(_, stats) => Statistics::F64(stats.clone()),
                    Column::I64(_, stats) => Statistics::I64(stats.clone()),
                    Column::U64(_, stats) => Statistics::U64(stats.clone()),
                    Column::Bool(_, stats) => Statistics::Bool(stats.clone()),
                    Column::String(_, stats) | Column::Tag(_, stats) => {
                        Statistics::String(stats.clone())
                    }
                };

                ColumnSummary {
                    name: column_name.to_string(),
                    stats,
                }
            })
            .collect()
    }
}

struct ColSelection<'a> {
    column_name: &'a str,
    column_id: u32,
}

/// Represets a set of column_name, column_index pairs
/// for a specific selection
struct TableColSelection<'a> {
    cols: Vec<ColSelection<'a>>,
}

impl<'a> TableColSelection<'a> {
    /// Sorts the columns by name
    fn sort_by_name(mut self) -> Self {
        self.cols.sort_by(|a, b| a.column_name.cmp(b.column_name));
        self
    }
}

#[cfg(test)]
mod tests {
    use internal_types::entry::test_helpers::lp_to_entry;

    use super::*;
    use tracker::MemRegistry;

    #[test]
    fn test_has_columns() {
        let registry = Arc::new(MemRegistry::new());
        let mut chunk = Chunk::new(42, registry.as_ref());
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let state_symbol = dictionary.id("state").unwrap();
        let new_symbol = dictionary.lookup_value_or_insert("not_a_columns");

        assert!(table.has_columns(None));

        let pred = ChunkIdSet::AtLeastOneMissing;
        assert!(!table.has_columns(Some(&pred)));

        let set = BTreeSet::<u32>::new();
        let pred = ChunkIdSet::Present(set);
        assert!(table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        let pred = ChunkIdSet::Present(set);
        assert!(table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(new_symbol);
        let pred = ChunkIdSet::Present(set);
        assert!(!table.has_columns(Some(&pred)));

        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        set.insert(new_symbol);
        let pred = ChunkIdSet::Present(set);
        assert!(!table.has_columns(Some(&pred)));
    }

    #[test]
    fn table_size() {
        let registry = Arc::new(MemRegistry::new());
        let mut chunk = Chunk::new(42, registry.as_ref());
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines.clone());
        assert_eq!(128, table.size());

        // doesn't double because of the stats overhead
        write_lines_to_table(&mut table, dictionary, lp_lines.clone());
        assert_eq!(224, table.size());

        // now make sure it increased by the same amount minus stats overhead
        write_lines_to_table(&mut table, dictionary, lp_lines);
        assert_eq!(320, table.size());
    }

    #[test]
    fn test_matches_table_name_predicate() {
        let registry = Arc::new(MemRegistry::new());
        let mut chunk = Chunk::new(42, registry.as_ref());
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("h2o"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];
        write_lines_to_table(&mut table, dictionary, lp_lines);

        let h2o_symbol = dictionary.id("h2o").unwrap();

        assert!(table.matches_table_name_predicate(None));

        let set = BTreeSet::new();
        assert!(!table.matches_table_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(h2o_symbol);
        assert!(table.matches_table_name_predicate(Some(&set)));

        // Some symbol that is not the same as h2o_symbol
        assert_ne!(37377, h2o_symbol);
        let mut set = BTreeSet::new();
        set.insert(37377);
        assert!(!table.matches_table_name_predicate(Some(&set)));
    }

    #[test]
    fn test_matches_column_name_predicate() {
        let registry = Arc::new(MemRegistry::new());
        let mut chunk = Chunk::new(42, registry.as_ref());
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("h2o"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4,awesomeness=1000 100",
            "h2o,state=MA,city=Boston temp=72.4,awesomeness=2000 250",
        ];
        write_lines_to_table(&mut table, dictionary, lp_lines);

        let state_symbol = dictionary.id("state").unwrap();
        let temp_symbol = dictionary.id("temp").unwrap();
        let awesomeness_symbol = dictionary.id("awesomeness").unwrap();

        assert!(table.matches_column_name_predicate(None));

        let set = BTreeSet::new();
        assert!(!table.matches_column_name_predicate(Some(&set)));

        // tag columns should not count
        let mut set = BTreeSet::new();
        set.insert(state_symbol);
        assert!(!table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(temp_symbol);
        assert!(table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(temp_symbol);
        set.insert(awesomeness_symbol);
        assert!(table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(temp_symbol);
        set.insert(awesomeness_symbol);
        set.insert(1337); // some other symbol, but that is ok
        assert!(table.matches_column_name_predicate(Some(&set)));

        let mut set = BTreeSet::new();
        set.insert(1337);
        assert!(!table.matches_column_name_predicate(Some(&set)));
    }

    #[test]
    fn test_to_arrow_schema_all() {
        let registry = Arc::new(MemRegistry::new());
        let mut chunk = Chunk::new(42, registry.as_ref());
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,uint_field=42u,bool_field=t,string_field=\"foo\" 100",
        ];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let selection = Selection::All;
        let actual_schema = table.schema(&chunk, selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("bool_field", ArrowDataType::Boolean)
            .tag("city")
            .field("float_field", ArrowDataType::Float64)
            .field("int_field", ArrowDataType::Int64)
            .tag("state")
            .field("string_field", ArrowDataType::Utf8)
            .timestamp()
            .field("uint_field", ArrowDataType::UInt64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn test_to_arrow_schema_subset() {
        let registry = Arc::new(MemRegistry::new());
        let mut chunk = Chunk::new(42, registry.as_ref());
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec!["h2o,state=MA,city=Boston float_field=70.4 100"];

        write_lines_to_table(&mut table, dictionary, lp_lines);

        let selection = Selection::Some(&["float_field"]);
        let actual_schema = table.schema(&chunk, selection).unwrap();
        let expected_schema = SchemaBuilder::new()
            .field("float_field", ArrowDataType::Float64)
            .build()
            .unwrap();

        assert_eq!(
            expected_schema, actual_schema,
            "Expected:\n{:#?}\nActual:\n{:#?}\n",
            expected_schema, actual_schema
        );
    }

    #[test]
    fn write_columns_validates_schema() {
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("foo"));

        let lp = "foo,t1=asdf iv=1i,uv=1u,fv=1.0,bv=true,sv=\"hi\" 1";
        let entry = lp_to_entry(&lp);
        table
            .write_columns(
                &mut dictionary,
                ClockValue::new(0),
                0,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .unwrap();

        let lp = "foo t1=\"string\" 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                ClockValue::new(0),
                0,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(
                &response,
                Error::InternalColumnTypeMismatch {
                    expected_column_type,
                    actual_column_type,
                    ..
                } if expected_column_type == "tag" && actual_column_type == "String"),
            format!("didn't match returned error: {:?}", response)
        );

        let lp = "foo iv=1u 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                ClockValue::new(0),
                0,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(&response, Error::InternalColumnTypeMismatch {expected_column_type, actual_column_type, ..} if expected_column_type == "i64" && actual_column_type == "u64"),
            format!("didn't match returned error: {:?}", response)
        );

        let lp = "foo fv=1i 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                ClockValue::new(0),
                0,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(&response, Error::InternalColumnTypeMismatch {expected_column_type, actual_column_type, ..} if expected_column_type == "f64" && actual_column_type == "i64"),
            format!("didn't match returned error: {:?}", response)
        );

        let lp = "foo bv=1 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                ClockValue::new(0),
                0,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(&response, Error::InternalColumnTypeMismatch {expected_column_type, actual_column_type, ..} if expected_column_type == "bool" && actual_column_type == "f64"),
            format!("didn't match returned error: {:?}", response)
        );

        let lp = "foo sv=true 1";
        let entry = lp_to_entry(&lp);
        let response = table
            .write_columns(
                &mut dictionary,
                ClockValue::new(0),
                0,
                entry
                    .partition_writes()
                    .unwrap()
                    .first()
                    .unwrap()
                    .table_batches()
                    .first()
                    .unwrap()
                    .columns(),
            )
            .err()
            .unwrap();
        assert!(
            matches!(&response, Error::InternalColumnTypeMismatch {expected_column_type, actual_column_type, ..} if expected_column_type == "String" && actual_column_type == "bool"),
            format!("didn't match returned error: {:?}", response)
        );
    }

    ///  Insert the line protocol lines in `lp_lines` into this table
    fn write_lines_to_table(table: &mut Table, dictionary: &mut Dictionary, lp_lines: Vec<&str>) {
        let lp_data = lp_lines.join("\n");
        let entry = lp_to_entry(&lp_data);

        for batch in entry
            .partition_writes()
            .unwrap()
            .first()
            .unwrap()
            .table_batches()
        {
            table
                .write_columns(dictionary, ClockValue::new(0), 0, batch.columns())
                .unwrap();
        }
    }
}
