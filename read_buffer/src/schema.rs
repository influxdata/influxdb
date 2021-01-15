use std::fmt::Display;

use crate::{column::LogicalDataType, AggregateType};

/// A schema that is used to track the names and semantics of columns returned
/// in results out of various operations on a row group.
///
/// This schema is useful for helping with displaying information in tests and
/// decorating Arrow record batches when results are converted before leaving
/// the read buffer.
#[derive(Default, PartialEq, Debug)]
pub struct ResultSchema {
    pub select_columns: Vec<(String, LogicalDataType)>,
    pub group_columns: Vec<(String, LogicalDataType)>,
    pub aggregate_columns: Vec<(String, AggregateType, LogicalDataType)>,
}

impl ResultSchema {
    pub fn select_column_names_iter(&self) -> impl Iterator<Item = &str> {
        self.select_columns.iter().map(|(name, _)| name.as_str())
    }

    pub fn group_column_names_iter(&self) -> impl Iterator<Item = &str> {
        self.group_columns.iter().map(|(name, _)| name.as_str())
    }

    pub fn aggregate_column_names_iter(&self) -> impl Iterator<Item = &str> {
        self.aggregate_columns
            .iter()
            .map(|(name, _, _)| name.as_str())
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
