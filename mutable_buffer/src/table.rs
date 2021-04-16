use std::{cmp, collections::BTreeMap, iter::FromIterator, sync::Arc};

use crate::{
    column,
    column::Column,
    dictionary::{Dictionary, Error as DictionaryError, DID},
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
            ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray,
            TimestampNanosecondArray, UInt64Array,
        },
        datatypes::DataType as ArrowDataType,
        record_batch::RecordBatch,
    },
};

#[derive(Debug, Snafu)]
pub enum Error {
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
        column_id: DID,
        expected_column_type: String,
        actual_column_type: String,
    },

    #[snafu(display("Internal error: unexpected aggregate request for None aggregate",))]
    InternalUnexpectedNoneAggregate {},

    #[snafu(display("Column name '{}' not found in dictionary of chunk", column_name,))]
    ColumnNameNotFoundInDictionary { column_name: String },

    #[snafu(display("Internal: Column id '{}' not found in dictionary", column_id,))]
    ColumnIdNotFoundInDictionary {
        column_id: DID,
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
    InternalNoColumnInIndex { column_name: String, column_id: DID },

    #[snafu(display("Error creating column from wal for column {}: {}", column, source))]
    CreatingFromWal {
        column: DID,
        source: crate::column::Error,
    },

    #[snafu(display("Error evaluating column predicate for column {}: {}", column, source))]
    ColumnPredicateEvaluation {
        column: DID,
        source: crate::column::Error,
    },

    #[snafu(display("Row insert to table {} missing column name", table))]
    ColumnNameNotInRow { table: DID },

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
    ColumnIdNotFound { id: DID, table_id: DID },
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone)]
pub struct Table {
    /// Name of the table as a DID in the chunk dictionary
    pub id: DID,

    /// Map of column id from the chunk dictionary to the column
    pub columns: BTreeMap<DID, Column>,
}

impl Table {
    pub fn new(id: DID) -> Self {
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
    pub(crate) fn column(&self, column_id: DID) -> Result<&Column> {
        self.columns.get(&column_id).context(ColumnIdNotFound {
            id: column_id,
            table_id: self.id,
        })
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
    fn all_columns_selection<'a>(
        &self,
        dictionary: &'a Dictionary,
    ) -> Result<TableColSelection<'a>> {
        let cols = self
            .columns
            .iter()
            .map(|(column_id, _)| {
                let column_name =
                    dictionary
                        .lookup_id(*column_id)
                        .context(ColumnIdNotFoundInDictionary {
                            column_id: *column_id,
                        })?;
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
        dictionary: &'a Dictionary,
        columns: &'a [&'a str],
    ) -> Result<TableColSelection<'a>> {
        let cols = columns
            .iter()
            .map(|&column_name| {
                let column_id = dictionary
                    .id(column_name)
                    .context(ColumnNameNotFoundInDictionary { column_name })?;

                Ok(ColSelection {
                    column_name,
                    column_id,
                })
            })
            .collect::<Result<_>>()?;

        Ok(TableColSelection { cols })
    }

    /// Converts this table to an arrow record batch.
    pub fn to_arrow(
        &self,
        dictionary: &Dictionary,
        selection: Selection<'_>,
    ) -> Result<RecordBatch> {
        // translate chunk selection into name/indexes:
        let selection = match selection {
            Selection::All => self.all_columns_selection(dictionary),
            Selection::Some(cols) => self.specific_columns_selection(dictionary, cols),
        }?;
        self.to_arrow_impl(dictionary, &selection)
    }

    pub fn schema(&self, dictionary: &Dictionary, selection: Selection<'_>) -> Result<Schema> {
        // translate chunk selection into name/indexes:
        let selection = match selection {
            Selection::All => self.all_columns_selection(dictionary),
            Selection::Some(cols) => self.specific_columns_selection(dictionary, cols),
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
        dictionary: &Dictionary,
        selection: &TableColSelection<'_>,
    ) -> Result<RecordBatch> {
        let mut columns = Vec::with_capacity(selection.cols.len());

        for col in &selection.cols {
            let column = self.column(col.column_id)?;

            let array: ArrayRef = match column {
                Column::String(vals, _) => {
                    let iter = vals.iter().map(|s| s.as_deref());
                    let array = StringArray::from_iter(iter);
                    Arc::new(array)
                }
                Column::Tag(vals, _) => {
                    let iter = vals.iter().map(|id| {
                        id.as_ref().map(|value_id| {
                            dictionary
                                .lookup_id(*value_id)
                                .expect("dictionary had mapping for tag value")
                        })
                    });

                    let array = StringArray::from_iter(iter);
                    Arc::new(array)
                }
                Column::F64(vals, _) => {
                    let array = Float64Array::from_iter(vals.iter());
                    Arc::new(array)
                }
                Column::I64(vals, _) => {
                    if col.column_name == TIME_COLUMN_NAME {
                        // TODO: there is no reason that this needs an owned copy of the Vec...
                        // should file a ticket with arrow to add something to avoid the clone
                        let array = TimestampNanosecondArray::from_opt_vec(vals.clone(), None);
                        Arc::new(array)
                    } else {
                        let array = Int64Array::from_iter(vals.iter());
                        Arc::new(array)
                    }
                }
                Column::U64(vals, _) => {
                    let array = UInt64Array::from_iter(vals.iter());
                    Arc::new(array)
                }
                Column::Bool(vals, _) => {
                    let array = BooleanArray::from_iter(vals.iter());
                    Arc::new(array)
                }
            };

            columns.push(array);
        }

        let schema = self.schema_impl(selection)?.into();

        RecordBatch::try_new(schema, columns).context(ArrowError {})
    }

    pub fn stats(&self, dictionary: &Dictionary) -> Vec<ColumnSummary> {
        self.columns
            .iter()
            .map(|(column_id, c)| {
                let column_name = dictionary
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
    column_id: DID,
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

    #[test]
    fn table_size() {
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston temp=70.4 100",
            "h2o,state=MA,city=Boston temp=72.4 250",
        ];

        write_lines_to_table(&mut table, &mut dictionary, lp_lines.clone());
        assert_eq!(112, table.size());

        // doesn't double because of the stats overhead
        write_lines_to_table(&mut table, &mut dictionary, lp_lines.clone());
        assert_eq!(192, table.size());

        // now make sure it increased by the same amount minus stats overhead
        write_lines_to_table(&mut table, &mut dictionary, lp_lines);
        assert_eq!(272, table.size());
    }

    #[test]
    fn test_to_arrow_schema_all() {
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec![
            "h2o,state=MA,city=Boston float_field=70.4,int_field=8i,uint_field=42u,bool_field=t,string_field=\"foo\" 100",
        ];

        write_lines_to_table(&mut table, &mut dictionary, lp_lines);

        let selection = Selection::All;
        let actual_schema = table.schema(&dictionary, selection).unwrap();
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
        let mut dictionary = Dictionary::new();
        let mut table = Table::new(dictionary.lookup_value_or_insert("table_name"));

        let lp_lines = vec!["h2o,state=MA,city=Boston float_field=70.4 100"];

        write_lines_to_table(&mut table, &mut dictionary, lp_lines);

        let selection = Selection::Some(&["float_field"]);
        let actual_schema = table.schema(&dictionary, selection).unwrap();
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
