use delorean_generated_types::wal as wb;
use delorean_line_parser::FieldValue;
use snafu::Snafu;

use crate::wal::type_description;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Don't know how to insert a column of type {}", inserted_value_type))]
    UnknownColumnType { inserted_value_type: String },

    #[snafu(display("Types did not match. Expected: {}, got: {}", expected, got))]
    TypeMismatch { expected: String, got: String },

    #[snafu(display("InternalError: Applying i64 range on a column with non-i64 type"))]
    InternalTypeMismatchForTimePredicate {},
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

// ColumnValue is a temporary holder of the column ID (name to dict mapping) and its value
#[derive(Debug)]
pub struct ColumnValue<'a> {
    pub id: u32,
    pub column: &'a str,
    pub value: Value<'a>,
}

#[derive(Debug)]
pub enum Value<'a> {
    TagValue(u32, &'a str),
    FieldValue(&'a delorean_line_parser::FieldValue<'a>),
}

impl<'a> Value<'a> {
    pub fn type_description(&self) -> &'static str {
        match self {
            Value::TagValue(_, _) => "tag",
            Value::FieldValue(FieldValue::I64(_)) => "i64",
            Value::FieldValue(FieldValue::F64(_)) => "f64",
            Value::FieldValue(FieldValue::Boolean(_)) => "bool",
            Value::FieldValue(FieldValue::String(_)) => "String",
        }
    }
}

#[derive(Debug)]
/// Stores the actual data for columns in a partition
///
/// TODO: add some summary statistics (like min/max for example)
pub enum Column {
    F64(Vec<Option<f64>>),
    I64(Vec<Option<i64>>),
    String(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
    Tag(Vec<Option<u32>>),
}
impl Column {
    pub fn new_from_wal(capacity: usize, value_type: wb::ColumnValue) -> Result<Self> {
        use wb::ColumnValue::*;

        Ok(match value_type {
            F64Value => Self::F64(vec![None; capacity]),
            I64Value => Self::I64(vec![None; capacity]),
            StringValue => Self::String(vec![None; capacity]),
            BoolValue => Self::Bool(vec![None; capacity]),
            TagValue => Self::Tag(vec![None; capacity]),
            _ => {
                return UnknownColumnType {
                    inserted_value_type: type_description(value_type),
                }
                .fail()
            }
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::F64(v) => v.len(),
            Self::I64(v) => v.len(),
            Self::String(v) => v.len(),
            Self::Bool(v) => v.len(),
            Self::Tag(v) => v.len(),
        }
    }

    pub fn type_description(&self) -> &'static str {
        match self {
            Self::F64(_) => "f64",
            Self::I64(_) => "i64",
            Self::String(_) => "String",
            Self::Bool(_) => "bool",
            Self::Tag(_) => "tag",
        }
    }

    // TODO: have type mismatches return helpful error
    pub fn matches_type(&self, val: &ColumnValue<'_>) -> bool {
        match (self, &val.value) {
            (Self::Tag(_), Value::TagValue(_, _)) => true,
            (col, Value::FieldValue(field)) => match (col, field) {
                (Self::F64(_), FieldValue::F64(_)) => true,
                (Self::I64(_), FieldValue::I64(_)) => true,
                (Self::Bool(_), FieldValue::Boolean(_)) => true,
                (Self::String(_), FieldValue::String(_)) => true,
                _ => false,
            },
            _ => false,
        }
    }

    pub fn push(&mut self, value: &Value<'_>) -> Result<()> {
        match (self, value) {
            (Self::Tag(vals), Value::TagValue(id, _)) => vals.push(Some(*id)),
            (Self::String(vals), Value::FieldValue(FieldValue::String(val))) => {
                vals.push(Some(val.to_string()))
            }
            (Self::Bool(vals), Value::FieldValue(FieldValue::Boolean(val))) => {
                vals.push(Some(*val))
            }
            (Self::I64(vals), Value::FieldValue(FieldValue::I64(val))) => vals.push(Some(*val)),
            (Self::F64(vals), Value::FieldValue(FieldValue::F64(val))) => vals.push(Some(*val)),
            (column, value) => {
                return TypeMismatch {
                    expected: column.type_description(),
                    got: value.type_description(),
                }
                .fail()
            }
        }

        Ok(())
    }

    // push_none_if_len_equal will add a None value to the end of the Vec of values if the
    // length is equal to the passed in value. This is used to ensure columns are all the same length.
    pub fn push_none_if_len_equal(&mut self, len: usize) {
        match self {
            Self::F64(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::I64(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::String(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::Bool(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::Tag(v) => {
                if v.len() == len {
                    v.push(None);
                }
            }
        }
    }

    /// Returns true if any rows are within the range [min_value,
    /// max_value). Inclusive of `start`, exclusive of `end`
    pub fn has_i64_range(&self, start: i64, end: i64) -> Result<bool> {
        match self {
            Self::I64(v) => {
                for val in v.iter() {
                    if let Some(val) = val {
                        if start <= *val && *val < end {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            _ => InternalTypeMismatchForTimePredicate {}.fail(),
        }
    }

    /// Returns true if there exists at least one row idx where this
    /// self[i] is within the range [min_value, max_value). Inclusive
    /// of `start`, exclusive of `end` and where col[i] is non null
    pub fn has_non_null_i64_range<T>(
        &self,
        column: &[Option<T>],
        start: i64,
        end: i64,
    ) -> Result<bool> {
        match self {
            Self::I64(v) => {
                for (index, val) in v.iter().enumerate() {
                    if let Some(val) = val {
                        if start <= *val && *val < end && column[index].is_some() {
                            return Ok(true);
                        }
                    }
                }
                Ok(false)
            }
            _ => InternalTypeMismatchForTimePredicate {}.fail(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type TestError = Box<dyn std::error::Error + Send + Sync + 'static>;
    type Result<T = (), E = TestError> = std::result::Result<T, E>;

    #[test]
    fn test_has_i64_range() -> Result {
        let col = Column::I64(vec![]);
        assert!(!col.has_i64_range(-1, 0)?);

        let col = Column::I64(vec![Some(1), None, Some(2)]);
        assert!(!col.has_i64_range(-1, 0)?);
        assert!(!col.has_i64_range(0, 1)?);
        assert!(col.has_i64_range(1, 2)?);
        assert!(col.has_i64_range(2, 3)?);
        assert!(!col.has_i64_range(3, 4)?);

        let col = Column::I64(vec![Some(2), None, Some(1)]);
        assert!(!col.has_i64_range(-1, 0)?);
        assert!(!col.has_i64_range(0, 1)?);
        assert!(col.has_i64_range(1, 2)?);
        assert!(col.has_i64_range(2, 3)?);
        assert!(!col.has_i64_range(3, 4)?);

        Ok(())
    }

    #[test]
    fn test_has_i64_range_does_not_panic() -> Result {
        // providing the wrong column type should get an internal error, not a panic
        let col = Column::F64(vec![]);
        let res = col.has_i64_range(-1, 0);
        assert!(res.is_err());
        let res_string = format!("{:?}", res);
        let expected = "InternalTypeMismatchForTimePredicate";
        assert!(
            res_string.contains(expected),
            "Did not find expected text '{}' in '{}'",
            expected,
            res_string
        );
        Ok(())
    }

    #[test]
    fn test_has_non_null_i64_range_() -> Result {
        let col = Column::I64(vec![]);
        assert!(!col.has_non_null_i64_range::<u32>(&[], -1, 0)?);

        let none_col: Vec<Option<u32>> = vec![None, None, None];
        let some_col: Vec<Option<u32>> = vec![Some(0), Some(0), Some(0)];

        let col = Column::I64(vec![Some(1), None, Some(2)]);

        assert!(!col.has_non_null_i64_range(&some_col, -1, 0)?);
        assert!(!col.has_non_null_i64_range(&some_col, 0, 1)?);
        assert!(col.has_non_null_i64_range(&some_col, 1, 2)?);
        assert!(col.has_non_null_i64_range(&some_col, 2, 3)?);
        assert!(!col.has_non_null_i64_range(&some_col, 3, 4)?);

        assert!(!col.has_non_null_i64_range(&none_col, -1, 0)?);
        assert!(!col.has_non_null_i64_range(&none_col, 0, 1)?);
        assert!(!col.has_non_null_i64_range(&none_col, 1, 2)?);
        assert!(!col.has_non_null_i64_range(&none_col, 2, 3)?);
        assert!(!col.has_non_null_i64_range(&none_col, 3, 4)?);

        Ok(())
    }
}
