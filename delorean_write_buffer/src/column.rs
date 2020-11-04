use delorean_generated_types::wal as wb;
use snafu::Snafu;

use crate::dictionary::Dictionary;
use data_types::{data::type_description, partition_metadata::Statistics};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Don't know how to insert a column of type {}", inserted_value_type))]
    UnknownColumnType { inserted_value_type: String },

    #[snafu(display(
        "Unable to insert {} type into a column of {}",
        inserted_value_type,
        existing_column_type
    ))]
    TypeMismatch {
        existing_column_type: String,
        inserted_value_type: String,
    },

    #[snafu(display("InternalError: Applying i64 range on a column with non-i64 type"))]
    InternalTypeMismatchForTimePredicate,
}
pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug)]
/// Stores the actual data for columns in a partition along with summary statistics
pub enum Column {
    F64(Vec<Option<f64>>, Statistics<f64>),
    I64(Vec<Option<i64>>, Statistics<i64>),
    String(Vec<Option<String>>, Statistics<String>),
    Bool(Vec<Option<bool>>, Statistics<bool>),
    Tag(Vec<Option<u32>>, Statistics<String>),
}

impl Column {
    pub fn with_value(
        dictionary: &mut Dictionary,
        capacity: usize,
        value: wb::Value<'_>,
    ) -> Result<Self> {
        use wb::ColumnValue::*;

        Ok(match value.value_type() {
            F64Value => {
                let val = value
                    .value_as_f64value()
                    .expect("f64 value should be present")
                    .value();
                let mut vals = vec![None; capacity];
                vals.push(Some(val));
                Self::F64(vals, Statistics::new(val))
            }
            I64Value => {
                let val = value
                    .value_as_i64value()
                    .expect("i64 value should be present")
                    .value();
                let mut vals = vec![None; capacity];
                vals.push(Some(val));
                Self::I64(vals, Statistics::new(val))
            }
            StringValue => {
                let val = value
                    .value_as_string_value()
                    .expect("string value should be present")
                    .value()
                    .expect("string must be present");
                let mut vals = vec![None; capacity];
                vals.push(Some(val.to_string()));
                Self::String(vals, Statistics::new(val.to_string()))
            }
            BoolValue => {
                let val = value
                    .value_as_bool_value()
                    .expect("bool value should be present")
                    .value();
                let mut vals = vec![None; capacity];
                vals.push(Some(val));
                Self::Bool(vals, Statistics::new(val))
            }
            TagValue => {
                let val = value
                    .value_as_tag_value()
                    .expect("tag value should be present")
                    .value()
                    .expect("tag value must have string value");
                let mut vals = vec![None; capacity];
                let id = dictionary.lookup_value_or_insert(val);
                vals.push(Some(id));
                Self::Tag(vals, Statistics::new(val.to_string()))
            }
            _ => {
                return UnknownColumnType {
                    inserted_value_type: type_description(value.value_type()),
                }
                .fail()
            }
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::F64(v, _) => v.len(),
            Self::I64(v, _) => v.len(),
            Self::String(v, _) => v.len(),
            Self::Bool(v, _) => v.len(),
            Self::Tag(v, _) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn type_description(&self) -> &'static str {
        match self {
            Self::F64(_, _) => "f64",
            Self::I64(_, _) => "i64",
            Self::String(_, _) => "String",
            Self::Bool(_, _) => "bool",
            Self::Tag(_, _) => "tag",
        }
    }

    pub fn push(&mut self, dictionary: &mut Dictionary, value: &wb::Value<'_>) -> Result<()> {
        let inserted = match self {
            Self::Tag(vals, stats) => match value.value_as_tag_value() {
                Some(tag) => {
                    let tag_value = tag.value().expect("tag must have string value");
                    let id = dictionary.lookup_value_or_insert(tag_value);
                    vals.push(Some(id));
                    Statistics::update_string(stats, tag_value);
                    true
                }
                None => false,
            },
            Self::String(vals, stats) => match value.value_as_string_value() {
                Some(str_val) => {
                    let str_val = str_val.value().expect("string must have value");
                    vals.push(Some(str_val.to_string()));
                    Statistics::update_string(stats, str_val);
                    true
                }
                None => false,
            },
            Self::Bool(vals, stats) => match value.value_as_bool_value() {
                Some(bool_val) => {
                    let bool_val = bool_val.value();
                    vals.push(Some(bool_val));
                    stats.update(bool_val);
                    true
                }
                None => false,
            },
            Self::I64(vals, stats) => match value.value_as_i64value() {
                Some(i64_val) => {
                    let i64_val = i64_val.value();
                    vals.push(Some(i64_val));
                    stats.update(i64_val);
                    true
                }
                None => false,
            },
            Self::F64(vals, stats) => match value.value_as_f64value() {
                Some(f64_val) => {
                    let f64_val = f64_val.value();
                    vals.push(Some(f64_val));
                    stats.update(f64_val);
                    true
                }
                None => false,
            },
        };

        if inserted {
            Ok(())
        } else {
            TypeMismatch {
                existing_column_type: self.type_description(),
                inserted_value_type: type_description(value.value_type()),
            }
            .fail()
        }
    }

    // push_none_if_len_equal will add a None value to the end of the Vec of values if the
    // length is equal to the passed in value. This is used to ensure columns are all the same length.
    pub fn push_none_if_len_equal(&mut self, len: usize) {
        match self {
            Self::F64(v, _) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::I64(v, _) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::String(v, _) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::Bool(v, _) => {
                if v.len() == len {
                    v.push(None);
                }
            }
            Self::Tag(v, _) => {
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
            Self::I64(_, stats) => {
                if stats.max < start || stats.min >= end {
                    Ok(false)
                } else {
                    Ok(true)
                }
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
            Self::I64(v, _) => {
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
        let mut stats = Statistics::new(1);
        stats.update(2);
        let col = Column::I64(vec![Some(1), None, Some(2)], stats.clone());
        assert!(!col.has_i64_range(-1, 0)?);
        assert!(!col.has_i64_range(0, 1)?);
        assert!(col.has_i64_range(1, 2)?);
        assert!(col.has_i64_range(2, 3)?);
        assert!(!col.has_i64_range(3, 4)?);

        let col = Column::I64(vec![Some(2), None, Some(1)], stats);
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
        let col = Column::F64(vec![Some(1.2)], Statistics::new(1.2));
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
        let none_col: Vec<Option<u32>> = vec![None, None, None];
        let some_col: Vec<Option<u32>> = vec![Some(0), Some(0), Some(0)];

        let mut stats = Statistics::new(1);
        stats.update(2);
        let col = Column::I64(vec![Some(1), None, Some(2)], stats);

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
