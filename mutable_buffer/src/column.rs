use generated_types::wal as wb;
use snafu::Snafu;

use crate::dictionary::Dictionary;
use arrow_deps::arrow::datatypes::DataType as ArrowDataType;
use data_types::partition_metadata::StatValues;
use internal_types::data::type_description;

use std::mem;

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

/// Stores the actual data for columns in a chunk along with summary
/// statistics
#[derive(Debug, Clone)]
pub enum Column {
    F64(Vec<Option<f64>>, StatValues<f64>),
    I64(Vec<Option<i64>>, StatValues<i64>),
    U64(Vec<Option<u64>>, StatValues<u64>),
    String(Vec<Option<String>>, StatValues<String>),
    Bool(Vec<Option<bool>>, StatValues<bool>),
    Tag(Vec<Option<u32>>, StatValues<String>),
}

impl Column {
    pub fn with_value(
        dictionary: &mut Dictionary,
        capacity: usize,
        value: wb::Value<'_>,
    ) -> Result<Self> {
        Ok(match value.value_type() {
            wb::ColumnValue::F64Value => {
                let val = value
                    .value_as_f64value()
                    .expect("f64 value should be present")
                    .value();
                let mut vals = vec![None; capacity];
                vals.push(Some(val));
                Self::F64(vals, StatValues::new(val))
            }
            wb::ColumnValue::I64Value => {
                let val = value
                    .value_as_i64value()
                    .expect("i64 value should be present")
                    .value();
                let mut vals = vec![None; capacity];
                vals.push(Some(val));
                Self::I64(vals, StatValues::new(val))
            }
            wb::ColumnValue::U64Value => {
                let val = value
                    .value_as_u64value()
                    .expect("u64 value should be present")
                    .value();
                let mut vals = vec![None; capacity];
                vals.push(Some(val));
                Self::U64(vals, StatValues::new(val))
            }
            wb::ColumnValue::StringValue => {
                let val = value
                    .value_as_string_value()
                    .expect("string value should be present")
                    .value()
                    .expect("string must be present");
                let mut vals = vec![None; capacity];
                vals.push(Some(val.to_string()));
                Self::String(vals, StatValues::new(val.to_string()))
            }
            wb::ColumnValue::BoolValue => {
                let val = value
                    .value_as_bool_value()
                    .expect("bool value should be present")
                    .value();
                let mut vals = vec![None; capacity];
                vals.push(Some(val));
                Self::Bool(vals, StatValues::new(val))
            }
            wb::ColumnValue::TagValue => {
                let val = value
                    .value_as_tag_value()
                    .expect("tag value should be present")
                    .value()
                    .expect("tag value must have string value");
                let mut vals = vec![None; capacity];
                let id = dictionary.lookup_value_or_insert(val);
                vals.push(Some(id));
                Self::Tag(vals, StatValues::new(val.to_string()))
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
            Self::U64(v, _) => v.len(),
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
            Self::U64(_, _) => "u64",
            Self::String(_, _) => "String",
            Self::Bool(_, _) => "bool",
            Self::Tag(_, _) => "tag",
        }
    }

    /// Return the arrow DataType for this column
    pub fn data_type(&self) -> ArrowDataType {
        match self {
            Self::F64(..) => ArrowDataType::Float64,
            Self::I64(..) => ArrowDataType::Int64,
            Self::U64(..) => ArrowDataType::UInt64,
            Self::String(..) => ArrowDataType::Utf8,
            Self::Bool(..) => ArrowDataType::Boolean,
            Self::Tag(..) => ArrowDataType::Utf8,
        }
    }

    pub fn push(&mut self, dictionary: &mut Dictionary, value: &wb::Value<'_>) -> Result<()> {
        let inserted = match self {
            Self::Tag(vals, stats) => match value.value_as_tag_value() {
                Some(tag) => {
                    let tag_value = tag.value().expect("tag must have string value");
                    let id = dictionary.lookup_value_or_insert(tag_value);
                    vals.push(Some(id));
                    StatValues::update_string(stats, tag_value);
                    true
                }
                None => false,
            },
            Self::String(vals, stats) => match value.value_as_string_value() {
                Some(str_val) => {
                    let str_val = str_val.value().expect("string must have value");
                    vals.push(Some(str_val.to_string()));
                    StatValues::update_string(stats, str_val);
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
            Self::U64(vals, stats) => match value.value_as_u64value() {
                Some(u64_val) => {
                    let u64_val = u64_val.value();
                    vals.push(Some(u64_val));
                    stats.update(u64_val);
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

    // push_none_if_len_equal will add a None value to the end of the Vec of values
    // if the length is equal to the passed in value. This is used to ensure
    // columns are all the same length.
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
            Self::U64(v, _) => {
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

    /// Return true of this column's type is a Tag
    pub fn is_tag(&self) -> bool {
        matches!(self, Self::Tag(..))
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

    /// The approximate memory size of the data in the column. Note that
    /// the space taken for the tag string values is represented in
    /// the dictionary size in the chunk that holds the table that has this
    /// column. The size returned here is only for their identifiers.
    pub fn size(&self) -> usize {
        match self {
            Self::F64(v, stats) => {
                mem::size_of::<Option<f64>>() * v.len() + mem::size_of_val(&stats)
            }
            Self::I64(v, stats) => {
                mem::size_of::<Option<i64>>() * v.len() + mem::size_of_val(&stats)
            }
            Self::U64(v, stats) => {
                mem::size_of::<Option<u64>>() * v.len() + mem::size_of_val(&stats)
            }
            Self::Bool(v, stats) => {
                mem::size_of::<Option<bool>>() * v.len() + mem::size_of_val(&stats)
            }
            Self::Tag(v, stats) => {
                mem::size_of::<Option<u32>>() * v.len() + mem::size_of_val(&stats)
            }
            Self::String(v, stats) => {
                let string_bytes_size = v
                    .iter()
                    .fold(0, |acc, val| acc + val.as_ref().map_or(0, |s| s.len()));
                let vec_pointer_sizes = mem::size_of::<Option<String>>() * v.len();
                string_bytes_size + vec_pointer_sizes + mem::size_of_val(&stats)
            }
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
        let mut stats = StatValues::new(1);
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
        let col = Column::F64(vec![Some(1.2)], StatValues::new(1.2));
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

        let mut stats = StatValues::new(1);
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

    #[test]
    fn column_size() {
        let i64col = Column::I64(vec![Some(1), Some(1)], StatValues::new(1));
        assert_eq!(40, i64col.size());

        let f64col = Column::F64(vec![Some(1.1), Some(1.1), Some(1.1)], StatValues::new(1.1));
        assert_eq!(56, f64col.size());

        let boolcol = Column::Bool(vec![Some(true)], StatValues::new(true));
        assert_eq!(9, boolcol.size());

        let tagcol = Column::Tag(
            vec![Some(1), Some(1), Some(1), Some(1)],
            StatValues::new("foo".to_string()),
        );
        assert_eq!(40, tagcol.size());

        let stringcol = Column::String(
            vec![Some("foo".to_string()), Some("hello world".to_string())],
            StatValues::new("foo".to_string()),
        );
        assert_eq!(70, stringcol.size());
    }
}
