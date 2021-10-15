use std::sync::Arc;

use arrow::{self, datatypes::SchemaRef};
use schema::TIME_COLUMN_NAME;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Error finding field column: {:?} in schema '{}'", column_name, source))]
    ColumnNotFoundForField {
        column_name: String,
        source: arrow::error::ArrowError,
    },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// Names for a field: a value field and the associated timestamp columns
#[derive(Debug, PartialEq)]
pub enum FieldColumns {
    /// All field columns share a timestamp column, named TIME_COLUMN_NAME
    SharedTimestamp(Vec<Arc<str>>),

    /// Each field has a potentially different timestamp column
    // (value_name, timestamp_name)
    DifferentTimestamp(Vec<(Arc<str>, Arc<str>)>),
}

impl From<Vec<Arc<str>>> for FieldColumns {
    fn from(v: Vec<Arc<str>>) -> Self {
        Self::SharedTimestamp(v)
    }
}

impl From<Vec<(Arc<str>, Arc<str>)>> for FieldColumns {
    fn from(v: Vec<(Arc<str>, Arc<str>)>) -> Self {
        Self::DifferentTimestamp(v)
    }
}

impl From<Vec<&str>> for FieldColumns {
    fn from(v: Vec<&str>) -> Self {
        let v = v.into_iter().map(Arc::from).collect();

        Self::SharedTimestamp(v)
    }
}

impl From<&[&str]> for FieldColumns {
    fn from(v: &[&str]) -> Self {
        let v = v.iter().map(|v| Arc::from(*v)).collect();

        Self::SharedTimestamp(v)
    }
}

/// Column indexes for a field: a value and corresponding timestamp
#[derive(Debug, PartialEq, Clone)]
pub struct FieldIndex {
    pub value_index: usize,
    pub timestamp_index: usize,
}

#[derive(Debug, PartialEq, Clone)]
pub struct FieldIndexes {
    inner: Arc<Vec<FieldIndex>>,
}

impl FieldIndexes {
    /// Create FieldIndexes where each field has the same timestamp
    /// and different value index
    pub fn from_timestamp_and_value_indexes(
        timestamp_index: usize,
        value_indexes: &[usize],
    ) -> Self {
        value_indexes
            .iter()
            .map(|&value_index| FieldIndex {
                value_index,
                timestamp_index,
            })
            .collect::<Vec<_>>()
            .into()
    }

    /// Convert a slice of pairs (value_index, time_index) into
    /// FieldIndexes
    pub fn from_slice(v: &[(usize, usize)]) -> Self {
        let inner = v
            .iter()
            .map(|&(value_index, timestamp_index)| FieldIndex {
                value_index,
                timestamp_index,
            })
            .collect();

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn as_slice(&self) -> &[FieldIndex] {
        self.inner.as_ref()
    }

    pub fn iter(&self) -> impl Iterator<Item = &FieldIndex> {
        self.as_slice().iter()
    }
}

impl From<Vec<FieldIndex>> for FieldIndexes {
    fn from(list: Vec<FieldIndex>) -> Self {
        Self {
            inner: Arc::new(list),
        }
    }
}

impl FieldIndexes {
    // look up which column index correponds to each column name
    pub fn names_to_indexes(schema: &SchemaRef, column_names: &[Arc<str>]) -> Result<Vec<usize>> {
        column_names
            .iter()
            .map(|column_name| {
                schema
                    .index_of(&*column_name)
                    .context(ColumnNotFoundForField {
                        column_name: column_name.as_ref(),
                    })
            })
            .collect()
    }

    /// Translate the field columns into pairs of (field_index, timestamp_index)
    pub fn from_field_columns(schema: &SchemaRef, field_columns: &FieldColumns) -> Result<Self> {
        let indexes = match field_columns {
            FieldColumns::SharedTimestamp(field_names) => {
                let timestamp_index =
                    schema
                        .index_of(TIME_COLUMN_NAME)
                        .context(ColumnNotFoundForField {
                            column_name: TIME_COLUMN_NAME,
                        })?;

                Self::names_to_indexes(schema, field_names)?
                    .into_iter()
                    .map(|field_index| FieldIndex {
                        value_index: field_index,
                        timestamp_index,
                    })
                    .collect::<Vec<_>>()
                    .into()
            }
            FieldColumns::DifferentTimestamp(fields_and_timestamp_names) => {
                fields_and_timestamp_names
                    .iter()
                    .map(|(field_name, timestamp_name)| {
                        let field_index =
                            schema
                                .index_of(&*field_name)
                                .context(ColumnNotFoundForField {
                                    column_name: field_name.as_ref(),
                                })?;

                        let timestamp_index =
                            schema
                                .index_of(timestamp_name)
                                .context(ColumnNotFoundForField {
                                    column_name: TIME_COLUMN_NAME,
                                })?;

                        Ok(FieldIndex {
                            value_index: field_index,
                            timestamp_index,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?
                    .into()
            }
        };
        Ok(indexes)
    }
}
