use generated_types::wal as wb;

use std::{
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
    partition_metadata::{ColumnSummary, Statistics},
    schema::{builder::SchemaBuilder, Schema},
    selection::Selection,
    TIME_COLUMN_NAME,
};
use snafu::{OptionExt, ResultExt, Snafu};

use arrow_deps::{
    arrow,
    arrow::{
        array::{ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder},
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
        source: data_types::schema::builder::Error,
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

    fn append_row(
        &mut self,
        dictionary: &mut Dictionary,
        values: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Value<'_>>>,
    ) -> Result<()> {
        let row_count = self.row_count();

        // insert new columns and validate existing ones
        for value in values {
            let column_name = value
                .column()
                .context(ColumnNameNotInRow { table: self.id })?;
            let column_id = dictionary.lookup_value_or_insert(column_name);

            let column = match self.columns.get_mut(&column_id) {
                Some(col) => col,
                None => {
                    // Add the column and make all values for existing rows None
                    self.columns.insert(
                        column_id,
                        Column::with_value(dictionary, row_count, value)
                            .context(CreatingFromWal { column: column_id })?,
                    );

                    continue;
                }
            };

            column.push(dictionary, &value).context(ColumnError {
                column: column_name,
            })?;
        }

        // make sure all the columns are of the same length
        for col in self.columns.values_mut() {
            col.push_none_if_len_equal(row_count);
        }

        Ok(())
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

    pub fn append_rows(
        &mut self,
        dictionary: &mut Dictionary,
        rows: &flatbuffers::Vector<'_, flatbuffers::ForwardsUOffset<wb::Row<'_>>>,
    ) -> Result<()> {
        for row in rows {
            if let Some(values) = row.values() {
                self.append_row(dictionary, &values)?;
            }
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

    pub fn stats(&self, chunk: &Chunk) -> Result<Vec<ColumnSummary>> {
        let mut summaries = Vec::with_capacity(self.columns.len());

        for (column_id, c) in &self.columns {
            let column_name =
                chunk
                    .dictionary
                    .lookup_id(*column_id)
                    .context(ColumnIdNotFoundInDictionary {
                        column_id: *column_id,
                        chunk: chunk.id,
                    })?;

            let stats = match c {
                Column::F64(_, stats) => Statistics::F64(stats.clone()),
                Column::I64(_, stats) => Statistics::I64(stats.clone()),
                Column::Bool(_, stats) => Statistics::Bool(stats.clone()),
                Column::String(_, stats) | Column::Tag(_, stats) => {
                    Statistics::String(stats.clone())
                }
            };

            summaries.push(ColumnSummary {
                name: column_name.to_string(),
                stats,
            });
        }

        Ok(summaries)
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

    use data_types::data::split_lines_into_write_entry_partitions;
    use influxdb_line_protocol::{parse_lines, ParsedLine};

    use super::*;

    #[test]
    fn test_has_columns() {
        let mut chunk = Chunk::new(42);
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
        let mut chunk = Chunk::new(42);
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
        let mut chunk = Chunk::new(42);
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
        let mut chunk = Chunk::new(42);
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
        let mut chunk = Chunk::new(42);
        let dictionary = &mut chunk.dictionary;
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,bool_field=t,string_field=\"foo\" 100",
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
        let mut chunk = Chunk::new(42);
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

    ///  Insert the line protocol lines in `lp_lines` into this table
    fn write_lines_to_table(table: &mut Table, dictionary: &mut Dictionary, lp_lines: Vec<&str>) {
        let lp_data = lp_lines.join("\n");

        let lines: Vec<_> = parse_lines(&lp_data).map(|l| l.unwrap()).collect();

        let data = split_lines_into_write_entry_partitions(chunk_key_func, &lines);

        let batch = flatbuffers::get_root::<wb::WriteBufferBatch<'_>>(&data);
        let entries = batch.entries().expect("at least one entry");

        for entry in entries {
            let table_batches = entry.table_batches().expect("there were table batches");
            for batch in table_batches {
                let rows = batch.rows().expect("Had rows in the batch");
                table
                    .append_rows(dictionary, &rows)
                    .expect("Appended the row");
            }
        }
    }

    fn chunk_key_func(_: &ParsedLine<'_>) -> String {
        String::from("the_chunk_key")
    }
}
