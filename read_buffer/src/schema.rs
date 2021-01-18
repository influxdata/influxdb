use std::fmt::Display;

use arrow_deps::arrow;

/// A schema that is used to track the names and semantics of columns returned
/// in results out of various operations on a row group.
///
/// This schema is useful for helping with displaying information in tests and
/// decorating Arrow record batches when results are converted before leaving
/// the read buffer.
#[derive(Default, PartialEq, Debug)]
pub struct ResultSchema {
    pub select_columns: Vec<(ColumnType, LogicalDataType)>,
    pub group_columns: Vec<(ColumnType, LogicalDataType)>,
    pub aggregate_columns: Vec<(ColumnType, AggregateType, LogicalDataType)>,
}

impl ResultSchema {
    pub fn select_column_names_iter(&self) -> impl Iterator<Item = &String> {
        self.select_columns.iter().map(|(name, _)| match name {
            ColumnType::Tag(name) => name,
            ColumnType::Field(name) => name,
            ColumnType::Timestamp(name) => name,
            ColumnType::Other(name) => name,
        })
    }

    pub fn group_column_names_iter(&self) -> impl Iterator<Item = &String> {
        self.group_columns.iter().map(|(name, _)| match name {
            ColumnType::Tag(name) => name,
            ColumnType::Field(name) => name,
            ColumnType::Timestamp(name) => name,
            ColumnType::Other(name) => name,
        })
    }

    pub fn aggregate_column_names_iter(&self) -> impl Iterator<Item = &String> {
        self.aggregate_columns
            .iter()
            .map(|(name, _, _)| match name {
                ColumnType::Tag(name) => name,
                ColumnType::Field(name) => name,
                ColumnType::Timestamp(name) => name,
                ColumnType::Other(name) => name,
            })
    }
}

/// Effectively emits a header line for a CSV-like table.
impl Display for ResultSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // do we need to emit the group by and aggregate columns?
        let has_group_and_agg = !self.group_columns.is_empty();

        for (i, (name, _)) in self.select_columns.iter().enumerate() {
            if has_group_and_agg || i < self.select_columns.len() - 1 {
                write!(f, "{},", name)?;
            } else if !has_group_and_agg {
                return write!(f, "{}", name); // last value in header row
            }
        }

        // write out group by columns
        for (i, (name, _)) in self.group_columns.iter().enumerate() {
            write!(f, "{},", name)?;
        }

        // finally, emit the aggregate columns
        for (i, (col_name, col_agg, _)) in self.aggregate_columns.iter().enumerate() {
            write!(f, "{}_{}", col_name, col_agg)?;

            if i < self.aggregate_columns.len() - 1 {
                write!(f, ",")?;
            }
        }
        writeln!(f)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
/// The logical data-type for a column.
pub enum LogicalDataType {
    Integer,  // Signed integer
    Unsigned, // Unsigned integer
    Float,    //
    String,   // UTF-8 valid string
    Binary,   // Arbitrary collection of bytes
    Boolean,  //
}

impl LogicalDataType {
    pub fn to_arrow_datatype(&self) -> arrow::datatypes::DataType {
        match &self {
            LogicalDataType::Integer => arrow::datatypes::DataType::Int64,
            LogicalDataType::Unsigned => arrow::datatypes::DataType::UInt64,
            LogicalDataType::Float => arrow::datatypes::DataType::Float64,
            LogicalDataType::String => arrow::datatypes::DataType::Utf8,
            LogicalDataType::Binary => arrow::datatypes::DataType::Binary,
            LogicalDataType::Boolean => arrow::datatypes::DataType::Boolean,
        }
    }
}

/// These variants describe supported aggregates that can applied to columnar
/// data in the Read Buffer.
#[derive(Copy, Clone, PartialEq, Debug)]
pub enum AggregateType {
    Count,
    First,
    Last,
    Min,
    Max,
    Sum,
    /* TODO - support:
     * Distinct - (edd): not sure this counts as an aggregations. Seems more like a special
     * filter. CountDistinct
     * Percentile */
}

impl std::fmt::Display for AggregateType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                AggregateType::Count => "count",
                AggregateType::First => "first",
                AggregateType::Last => "last",
                AggregateType::Min => "min",
                AggregateType::Max => "max",
                AggregateType::Sum => "sum",
            }
        )
    }
}

/// Describes the semantic meaning of the column in a set of results. That is,
/// whether the column is a "tag", "field", "timestamp", or "other".
#[derive(PartialEq, Debug, PartialOrd)]
pub enum ColumnType {
    Tag(String),
    Field(String),
    Timestamp(String),
    Other(String),
}

impl ColumnType {
    pub fn as_str(&self) -> &str {
        match self {
            ColumnType::Tag(name) => name.as_str(),
            ColumnType::Field(name) => name.as_str(),
            ColumnType::Timestamp(name) => name.as_str(),
            ColumnType::Other(name) => name.as_str(),
        }
    }
}

impl Display for ColumnType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                ColumnType::Tag(name) => name,
                ColumnType::Field(name) => name,
                ColumnType::Timestamp(name) => name,
                ColumnType::Other(name) => name,
            }
        )
    }
}
