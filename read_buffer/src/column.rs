pub mod boolean;
pub mod cmp;
pub mod encoding;
pub mod float;
pub mod integer;
pub mod string;

use std::{collections::BTreeSet, mem::size_of};

use croaring::Bitmap;
use either::Either;

use arrow::array::Array;

use crate::schema::LogicalDataType;
use crate::value::{EncodedValues, OwnedValue, Scalar, Value, Values};
use boolean::BooleanEncoding;
use encoding::{bool, scalar};
use float::FloatEncoding;
use integer::IntegerEncoding;
use string::StringEncoding;

/// The possible logical types that column values can have. All values in a
/// column have the same physical type.
pub enum Column {
    // A column of dictionary run-length encoded values.
    String(MetaData<String>, StringEncoding),

    // A column of single or double-precision floating point values.
    Float(MetaData<f64>, FloatEncoding),

    // A column of signed integers, which may be encoded with a different
    // physical type to the logical type.
    Integer(MetaData<i64>, IntegerEncoding),

    // A column of unsigned integers, which may be encoded with a different
    // physical type to the logical type.
    Unsigned(MetaData<u64>, IntegerEncoding),

    // A column of boolean values.
    Bool(MetaData<bool>, BooleanEncoding),

    // These are TODO
    ByteArray(MetaData<Vec<u8>>, StringEncoding), // TODO - arbitrary bytes
}

impl Column {
    //
    //  Meta information about the column
    //

    /// The estimated size in bytes of the column that is held in memory.
    pub fn size(&self) -> usize {
        // Since `MetaData` is generic each value in the range can have a
        // different size, so just do the calculations here where we know each
        // `T`.
        match &self {
            Self::String(meta, data) => {
                let mut meta_size =
                    size_of::<Option<(String, String)>>() + size_of::<ColumnProperties>();
                if let Some((min, max)) = &meta.range {
                    meta_size += min.len() + max.len();
                };
                meta_size + data.size()
            }
            Self::Float(_, data) => {
                let meta_size = size_of::<Option<(f64, f64)>>() + size_of::<ColumnProperties>();
                meta_size + data.size()
            }
            Self::Integer(_, data) => {
                let meta_size = size_of::<Option<(i64, i64)>>() + size_of::<ColumnProperties>();
                meta_size + data.size()
            }
            Self::Unsigned(_, data) => {
                let meta_size = size_of::<Option<(u64, u64)>>() + size_of::<ColumnProperties>();
                meta_size + data.size()
            }
            Self::Bool(_, data) => {
                let meta_size = size_of::<Option<(bool, bool)>>() + size_of::<ColumnProperties>();
                meta_size + data.size()
            }
            Self::ByteArray(meta, data) => {
                let mut meta_size =
                    size_of::<Option<(Vec<u8>, Vec<u8>)>>() + size_of::<ColumnProperties>();
                if let Some((min, max)) = &meta.range {
                    meta_size += min.len() + max.len();
                };

                meta_size + data.size()
            }
        }
    }

    /// The estimated size in bytes of the contents of the column if it was not
    /// compressed, and was stored as a contiguous collection of elements. This
    /// method can provide a good approximation for the size of the column at
    /// the point of ingest in IOx.
    ///
    /// The `include_null` when set to true will result in any NULL values in
    /// the column having a fixed size for fixed-width data types, or the size
    /// of a pointer for heap-allocated data types such as strings. When set to
    /// `false` all NULL values are ignored from calculations.
    pub fn size_raw(&self, include_null: bool) -> usize {
        match &self {
            Self::String(_, data) => data.size_raw(include_null),
            Self::Float(_, data) => data.size_raw(include_null),
            Self::Integer(_, data) => data.size_raw(include_null),
            Self::Unsigned(_, data) => data.size_raw(include_null),
            Self::Bool(_, data) => data.size_raw(include_null),
            Self::ByteArray(_, data) => data.size_raw(include_null),
        }
    }

    pub fn num_rows(&self) -> u32 {
        match &self {
            Self::String(_, data) => data.num_rows(),
            Self::Float(_, data) => data.num_rows(),
            Self::Integer(_, data) => data.num_rows(),
            Self::Unsigned(_, data) => data.num_rows(),
            Self::Bool(_, data) => data.num_rows(),
            Self::ByteArray(_, data) => data.num_rows(),
        }
    }

    /// Returns the logical data-type associated with the column.
    pub fn logical_datatype(&self) -> LogicalDataType {
        match self {
            Self::String(_, _) => LogicalDataType::String,
            Self::Float(_, _) => LogicalDataType::Float,
            Self::Integer(_, _) => LogicalDataType::Integer,
            Self::Unsigned(_, _) => LogicalDataType::Unsigned,
            Self::Bool(_, _) => LogicalDataType::Boolean,
            Self::ByteArray(_, _) => LogicalDataType::Binary,
        }
    }

    /// Returns the (min, max)  values stored in this column
    pub fn column_range(&self) -> (OwnedValue, OwnedValue) {
        match &self {
            Self::String(meta, _) => match &meta.range {
                Some((min, max)) => (
                    OwnedValue::String(min.clone()),
                    OwnedValue::String(max.clone()),
                ),
                None => (OwnedValue::Null, OwnedValue::Null),
            },
            Self::Float(meta, _) => match meta.range {
                Some((min, max)) => (
                    OwnedValue::Scalar(Scalar::F64(min)),
                    OwnedValue::Scalar(Scalar::F64(max)),
                ),
                None => (OwnedValue::Null, OwnedValue::Null),
            },
            Self::Integer(meta, _) => match meta.range {
                Some((min, max)) => (
                    OwnedValue::Scalar(Scalar::I64(min)),
                    OwnedValue::Scalar(Scalar::I64(max)),
                ),
                None => (OwnedValue::Null, OwnedValue::Null),
            },
            Self::Unsigned(meta, _) => match meta.range {
                Some((min, max)) => (
                    OwnedValue::Scalar(Scalar::U64(min)),
                    OwnedValue::Scalar(Scalar::U64(max)),
                ),
                None => (OwnedValue::Null, OwnedValue::Null),
            },
            Self::Bool(meta, _) => match meta.range {
                Some((min, max)) => (OwnedValue::Boolean(min), OwnedValue::Boolean(max)),
                None => (OwnedValue::Null, OwnedValue::Null),
            },
            Self::ByteArray(_, _) => todo!(),
        }
    }

    // Returns statistics about the physical layout of columns
    pub(crate) fn storage_stats(&self) -> Statistics {
        match &self {
            Self::String(_, data) => data.storage_stats(),
            Self::Float(_, data) => data.storage_stats(),
            Self::Integer(_, data) => data.storage_stats(),
            Self::Unsigned(_, data) => data.storage_stats(),
            Self::Bool(_, data) => data.storage_stats(),
            Self::ByteArray(_, data) => data.storage_stats(),
        }
    }

    pub fn properties(&self) -> &ColumnProperties {
        match &self {
            Self::String(meta, _) => &meta.properties,
            Self::Float(meta, _) => &meta.properties,
            Self::Integer(meta, _) => &meta.properties,
            Self::Unsigned(meta, _) => &meta.properties,
            Self::Bool(meta, _) => &meta.properties,
            Self::ByteArray(meta, _) => &meta.properties,
        }
    }

    pub fn column_min(&self) -> Value<'_> {
        todo!()
    }

    pub fn column_max(&self) -> Value<'_> {
        todo!()
    }

    //
    //  Methods for getting materialised values.
    //

    /// The value present at the provided logical row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        assert!(
            row_id < self.num_rows(),
            "cannot read row {:?} from column with {:?} rows",
            row_id,
            self.num_rows()
        );

        match &self {
            Self::String(_, data) => data.value(row_id),
            Self::Float(_, data) => data.value(row_id),
            Self::Integer(_, data) => data.value(row_id),
            Self::Unsigned(_, data) => data.value(row_id),
            Self::Bool(_, data) => data.value(row_id),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    /// All values present at the provided logical row ids.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        assert!(
            row_ids.len() as u32 <= self.num_rows(),
            "too many row ids {:?} provided for column with {:?} rows",
            row_ids.len(),
            self.num_rows()
        );

        match &self {
            Self::String(_, data) => data.values(row_ids),
            Self::Float(_, data) => data.values(row_ids),
            Self::Integer(_, data) => data.values(row_ids),
            Self::Unsigned(_, data) => data.values(row_ids),
            Self::Bool(_, data) => data.values(row_ids),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    /// All logical values in the column.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::String(_, data) => data.all_values(),
            Self::Float(_, data) => data.all_values(),
            Self::Integer(_, data) => data.all_values(),
            Self::Unsigned(_, data) => data.all_values(),
            Self::Bool(_, data) => data.all_values(),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    /// The value present at the provided logical row id.
    pub fn decode_id(&self, encoded_id: u32) -> Value<'_> {
        match &self {
            Self::String(_, data) => data.decode_id(encoded_id),
            Self::ByteArray(_, _) => todo!(),
            _ => panic!("unsupported operation"),
        }
    }

    // The distinct set of values found at the logical row ids.
    pub fn distinct_values(&self, row_ids: impl Iterator<Item = u32>) -> BTreeSet<Option<&'_ str>> {
        match &self {
            Self::String(_, data) => data.distinct_values(row_ids),
            Self::Float(_, _) => {
                unimplemented!("distinct values is unimplemented for Float column")
            }
            Self::Integer(_, _) => {
                unimplemented!("distinct values is unimplemented for Integer column")
            }
            Self::Unsigned(_, _) => {
                unimplemented!("distinct values is unimplemented for Unsigned column")
            }
            Self::Bool(_, _) => {
                unimplemented!("distinct values is unimplemented for Bool column")
            }
            Self::ByteArray(_, _) => {
                unimplemented!("distinct values is unimplemented for ByteArray column")
            }
        }
    }

    //
    // Methods for getting encoded (compressed) values.
    //

    /// The encoded values found at the provided logical row ids.
    pub fn encoded_values(&self, row_ids: &[u32], dst: EncodedValues) -> EncodedValues {
        assert!(
            row_ids.len() as u32 <= self.num_rows(),
            "too many row ids {:?} provided for column with {:?} rows",
            row_ids.len(),
            self.num_rows()
        );

        match &self {
            Self::String(_, data) => match dst {
                EncodedValues::U32(dst) => EncodedValues::U32(data.encoded_values(row_ids, dst)),
                typ => unimplemented!("column type String does not support {:?} encoding", typ),
            },
            Self::Integer(_, data) => data.encoded_values(row_ids, dst),
            // Right now it only makes sense to expose encoded values on columns
            // that are being used for grouping operations, which typically is
            // are Tag Columns; they are `String` columns.
            _ => unimplemented!("encoded values on other column types not supported"),
        }
    }

    /// All encoded values in the column.
    pub fn all_encoded_values(&self, dst: EncodedValues) -> EncodedValues {
        match &self {
            Self::String(_, data) => match dst {
                EncodedValues::U32(dst) => EncodedValues::U32(data.all_encoded_values(dst)),
                _ => unimplemented!("column type does not support requested encoding"),
            },
            Self::Integer(_, data) => data.all_encoded_values(dst),
            // Right now it only makes sense to expose encoded values on columns
            // that are being used for grouping operations, which typically is
            // are Tag Columns (Strings) or Time Columns (Integers).
            _ => unimplemented!("encoded values on other column types not currently supported"),
        }
    }

    pub fn grouped_row_ids(&self) -> Either<Vec<&RowIDs>, Vec<RowIDs>> {
        match &self {
            Self::String(_, data) => data.group_row_ids(),
            _ => unimplemented!("grouping not yet implemented"),
        }
    }

    //
    // Methods for filtering
    //

    /// Determine the set of row ids that satisfy the predicate.
    pub fn row_ids_filter(
        &self,
        op: &cmp::Operator,
        value: &Value<'_>,
        dst: RowIDs,
    ) -> RowIDsOption {
        // If we can get an answer using only the meta-data on the column then
        // return that answer.
        match self.evaluate_predicate_on_meta(&op, &value) {
            PredicateMatch::None => return RowIDsOption::None(dst),
            PredicateMatch::All => return RowIDsOption::All(dst),
            PredicateMatch::SomeMaybe => {} // have to apply predicate to column
        }

        // Check the column for all rows that satisfy the predicate.
        let row_ids = match &self {
            Self::String(_, data) => data.row_ids_filter(op, value.str(), dst),
            Self::Float(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Self::Integer(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Self::Unsigned(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Self::Bool(_, data) => data.row_ids_filter(op, value.bool(), dst),
            Self::ByteArray(_, _) => todo!(),
        };

        if row_ids.is_empty() {
            return RowIDsOption::None(row_ids);
        }
        RowIDsOption::Some(row_ids)
    }

    /// Determine the set of row ids that satisfy both of the predicates.
    ///
    /// Note: this method is a special case for common range-based predicates
    /// that are often found on timestamp columns.
    pub fn row_ids_filter_range(
        &self,
        low: &(cmp::Operator, Value<'_>),
        high: &(cmp::Operator, Value<'_>),
        dst: RowIDs,
    ) -> RowIDsOption {
        let l = self.evaluate_predicate_on_meta(&low.0, &low.1);
        let h = self.evaluate_predicate_on_meta(&high.0, &high.1);
        match (l, h) {
            (PredicateMatch::All, PredicateMatch::All) => return RowIDsOption::All(dst),

            // One of the predicates can't be satisfied, therefore no rows will
            // match both predicates.
            (PredicateMatch::None, _) | (_, PredicateMatch::None) => {
                return RowIDsOption::None(dst)
            }

            // One of the predicates matches all rows so reduce the operation
            // to the other side.
            (PredicateMatch::SomeMaybe, PredicateMatch::All) => {
                return self.row_ids_filter(&low.0, &low.1, dst);
            }
            (PredicateMatch::All, PredicateMatch::SomeMaybe) => {
                return self.row_ids_filter(&high.0, &high.1, dst);
            }

            // Have to apply the predicates to the column to identify correct
            // set of rows.
            (PredicateMatch::SomeMaybe, PredicateMatch::SomeMaybe) => {}
        }

        // TODO(edd): figure out pooling of these
        let dst = RowIDs::Bitmap(Bitmap::create());

        let low_scalar = (&low.0, low.1.scalar());
        let high_scalar = (&high.0, high.1.scalar());

        // Check the column for all rows that satisfy the predicate.
        let row_ids = match &self {
            Self::String(_, _) => unimplemented!("not supported on string columns yet"),
            Self::Float(_, data) => data.row_ids_filter_range(low_scalar, high_scalar, dst),
            Self::Integer(_, data) => data.row_ids_filter_range(low_scalar, high_scalar, dst),
            Self::Unsigned(_, data) => {
                data.row_ids_filter_range((&low.0, low.1.scalar()), (&high.0, high.1.scalar()), dst)
            }
            Self::Bool(_, _) => unimplemented!("filter_range not supported on boolean column"),
            Self::ByteArray(_, _) => todo!(),
        };

        if row_ids.is_empty() {
            return RowIDsOption::None(row_ids);
        }
        RowIDsOption::Some(row_ids)
    }

    // Helper function to determine if the predicate matches either no rows or
    // all the rows in a column. This is determined by looking at the metadata
    // on the column.
    //
    // `None` indicates that the column may contain some matching rows and the
    // predicate should be directly applied to the column.
    fn evaluate_predicate_on_meta(&self, op: &cmp::Operator, value: &Value<'_>) -> PredicateMatch {
        match op {
            // When the predicate is == and the metadata range indicates the column
            // can't contain `value` then the column doesn't need to be read.
            cmp::Operator::Equal => {
                if !self.might_contain_value(&value) {
                    return PredicateMatch::None; // no rows are going to match.
                }
            }

            // When the predicate is one of {<, <=, >, >=} and the column doesn't
            // contain any null values, and the entire range of values satisfies the
            // predicate then the column doesn't need to be read.
            cmp::Operator::GT | cmp::Operator::GTE | cmp::Operator::LT | cmp::Operator::LTE => {
                if self.predicate_matches_all_values(&op, &value) {
                    return PredicateMatch::All;
                }
            }

            // When the predicate is != and the metadata range indicates that the
            // column can't possibly contain `value` then the predicate must
            // match all rows on the column.
            cmp::Operator::NotEqual => {
                if !self.might_contain_value(&value) {
                    return PredicateMatch::All; // all rows are going to match.
                }
            }
        }

        if self.predicate_matches_no_values(&op, &value) {
            return PredicateMatch::None;
        }

        // The predicate could match some values
        PredicateMatch::SomeMaybe
    }

    // Helper method to determine if the column possibly contains this value.
    //
    // TODO(edd): currently this only handles non-null values.
    fn might_contain_value(&self, value: &Value<'_>) -> bool {
        match &self {
            Self::String(meta, _) => {
                if let Value::String(other) = value {
                    meta.might_contain_value(*other)
                } else {
                    unreachable!("impossible value comparison");
                }
            }
            // breaking this down:
            //   * Extract a Scalar variant from `value`, which should panic if that's not possible;
            //   * Try to safely convert that scalar to a primitive value based on the logical type
            //     used for the metadata on the column.
            //   * If the value can't be safely converted then there is no way that said value could
            //     be stored in the column at all -> false.
            //   * Otherwise if the value falls inside the range of values in the column range then
            //     it may be in the column -> true.
            Self::Float(meta, _) => value
                .scalar()
                .try_as_f64()
                .map_or_else(|| false, |v| meta.might_contain_value(v)),
            Self::Integer(meta, _) => value
                .scalar()
                .try_as_i64()
                .map_or_else(|| false, |v| meta.might_contain_value(v)),
            Self::Unsigned(meta, _) => value
                .scalar()
                .try_as_u64()
                .map_or_else(|| false, |v| meta.might_contain_value(v)),
            Self::Bool(meta, _) => match value {
                Value::Null => false,
                Value::Boolean(b) => meta.might_contain_value(*b),
                v => panic!("cannot compare boolean to {:?}", v),
            },
            Self::ByteArray(_, _) => todo!(),
        }
    }

    // Helper method to determine if the predicate matches all the values in
    // the column.
    //
    // TODO(edd): this doesn't handle operators that compare to a NULL value yet.
    fn predicate_matches_all_values(&self, op: &cmp::Operator, value: &Value<'_>) -> bool {
        match &self {
            Self::String(meta, data) => {
                if data.contains_null() {
                    false
                } else if let Value::String(other) = value {
                    meta.might_match_all_values(op, *other)
                } else {
                    unreachable!("impossible value comparison");
                }
            }
            // breaking this down:
            //   * If the column contains null values then it's not possible for all values in the
            //     column to match the predicate.
            //   * Extract a Scalar variant from `value`, which should panic if that's not possible;
            //   * Try to safely convert that scalar to a primitive value based on the logical type
            //     used for the metadata on the column.
            //   * If the value can't be safely converted then -> false.
            //   * Otherwise if the value falls inside the range of values in the column range then
            //     check if all values satisfy the predicate.
            Self::Float(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_f64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, v))
            }
            Self::Integer(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_i64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, v))
            }
            Self::Unsigned(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_u64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, v))
            }
            Self::Bool(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                match value {
                    Value::Null => false,
                    Value::Boolean(b) => meta.might_match_all_values(op, *b),
                    v => panic!("cannot compare on boolean column using {:?}", v),
                }
            }
            Self::ByteArray(_, _) => todo!(),
        }
    }

    // Helper method to determine if the predicate can not possibly match any
    // values in the column.
    fn predicate_matches_no_values(&self, op: &cmp::Operator, value: &Value<'_>) -> bool {
        match &self {
            Self::String(meta, _) => {
                if let Value::String(other) = value {
                    meta.match_no_values(op, *other)
                } else {
                    unreachable!("impossible value comparison");
                }
            }
            // breaking this down:
            //   * Extract a Scalar variant from `value`, which should panic if that's not possible;
            //   * Convert that scalar to a primitive value based on the logical type used for the
            //     metadata on the column.
            //   * See if one can prove none of the column can match the predicate.
            Self::Float(meta, _) => meta.match_no_values(op, value.scalar().as_f64()),
            Self::Integer(meta, _) => meta.match_no_values(op, value.scalar().as_i64()),
            Self::Unsigned(meta, _) => meta.match_no_values(op, value.scalar().as_u64()),
            Self::Bool(meta, _) => meta.match_no_values(op, value.bool()),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    //
    // Methods for selecting
    //

    /// The minimum value present within the set of rows.
    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Self::String(_, data) => data.min(row_ids),
            Self::Float(_, data) => data.min(row_ids),
            Self::Integer(_, data) => data.min(row_ids),
            Self::Unsigned(_, data) => data.min(row_ids),
            Self::Bool(_, data) => data.min(row_ids),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    /// The minimum value present within the set of rows.
    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Self::String(_, data) => data.max(row_ids),
            Self::Float(_, data) => data.max(row_ids),
            Self::Integer(_, data) => data.max(row_ids),
            Self::Unsigned(_, data) => data.max(row_ids),
            Self::Bool(_, data) => data.max(row_ids),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    //
    // Methods for aggregating
    //

    /// The summation of all non-null values located at the provided rows.
    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Self::Float(_, data) => data.sum(row_ids),
            Self::Integer(_, data) => data.sum(row_ids),
            Self::Unsigned(_, data) => data.sum(row_ids),
            _ => panic!("cannot sum non-numerical column type"),
        }
    }

    /// The count of all non-null values located at the provided rows.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Self::String(_, data) => data.count(row_ids),
            Self::Float(_, data) => data.count(row_ids),
            Self::Integer(_, data) => data.count(row_ids),
            Self::Unsigned(_, data) => data.count(row_ids),
            Self::Bool(_, data) => data.count(row_ids),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    //
    // Methods for inspecting
    //

    /// Determines if this column contains any NULL values.
    pub fn contains_null(&self) -> bool {
        match &self {
            Self::String(_, data) => data.contains_null(),
            Self::Float(_, data) => data.contains_null(),
            Self::Integer(_, data) => data.contains_null(),
            Self::Unsigned(_, data) => data.contains_null(),
            Self::Bool(_, data) => data.contains_null(),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    /// Determines if the column has a non-null value at any of the provided
    /// row ids.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match &self {
            Self::String(_, data) => data.has_non_null_value(row_ids),
            Self::Float(_, data) => data.has_non_null_value(row_ids),
            Self::Integer(_, data) => data.has_non_null_value(row_ids),
            Self::Unsigned(_, data) => data.has_non_null_value(row_ids),
            Self::Bool(_, data) => data.has_non_null_value(row_ids),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    /// Determines if the column has a non-null value on any row.
    pub fn has_any_non_null_value(&self) -> bool {
        match &self {
            Self::String(_, data) => data.has_any_non_null_value(),
            Self::Float(_, data) => data.has_any_non_null_value(),
            Self::Integer(_, data) => data.has_any_non_null_value(),
            Self::Unsigned(_, data) => data.has_any_non_null_value(),
            Self::Bool(_, data) => data.has_any_non_null_value(),
            Self::ByteArray(_, _) => todo!(),
        }
    }

    /// Determines if the column contains any string values that are not present
    /// in the provided `values` argument.
    pub fn has_other_non_null_string_values(&self, values: &BTreeSet<String>) -> bool {
        match self {
            Self::String(_, data) => data.has_other_non_null_values(values),
            Self::Float(_, _) => unimplemented!("operation not supported on `Float` column"),
            Self::Integer(_, _) => unimplemented!("operation not supported on `Integer` column"),
            Self::Unsigned(_, _) => {
                unimplemented!("operation not supported on `Unsigned` column")
            }
            Self::Bool(_, _) => unimplemented!("operation not supported on `Bool` column"),
            Self::ByteArray(_, _) => {
                unimplemented!("operation not supported on `ByteArray` column")
            }
        }
    }
}

#[derive(Default, Debug, PartialEq)]
pub struct ColumnProperties {
    pub has_pre_computed_row_ids: bool,
}

#[derive(Default, Debug, PartialEq)]
// The meta-data for a column
pub struct MetaData<T>
where
    T: PartialOrd + std::fmt::Debug,
{
    // The minimum and maximum value for this column.
    range: Option<(T, T)>,

    properties: ColumnProperties,
}

impl<T: PartialOrd + std::fmt::Debug> MetaData<T> {
    fn might_contain_value<U>(&self, v: U) -> bool
    where
        U: Into<T>,
    {
        let u = U::into(v);
        match &self.range {
            Some(range) => range.0 <= u && u <= range.1,
            None => false,
        }
    }

    // Determines if it's possible that predicate would match all rows in the
    // column. It is up to the caller to determine if the column contains null
    // values, which would invalidate a truthful result.
    fn might_match_all_values<U>(&self, op: &cmp::Operator, v: U) -> bool
    where
        U: Into<T>,
    {
        let u = U::into(v);
        match &self.range {
            Some(range) => match op {
                // all values in column equal to v
                cmp::Operator::Equal => range.0 == range.1 && range.1 == u,
                // all values larger or smaller than v so can't contain v
                cmp::Operator::NotEqual => u < range.0 || u > range.1,
                // all values in column > v
                cmp::Operator::GT => range.0 > u,
                // all values in column >= v
                cmp::Operator::GTE => range.0 >= u,
                // all values in column < v
                cmp::Operator::LT => range.1 < u,
                // all values in column <= v
                cmp::Operator::LTE => range.1 <= u,
            },
            None => false, // only null values in column.
        }
    }

    // Determines if it can be shown that the predicate would not match any rows
    // in the column.
    fn match_no_values<U>(&self, op: &cmp::Operator, v: U) -> bool
    where
        U: Into<T>,
    {
        let u = U::into(v);
        match &self.range {
            Some(range) => match op {
                // no values are `v` so no rows will match `== v`
                cmp::Operator::Equal => range.0 == range.1 && range.1 != u,
                // all values are `v` so no rows will match `!= v`
                cmp::Operator::NotEqual => range.0 == range.1 && range.1 == u,
                // max value in column is `<= v` so no values can be `> v`
                cmp::Operator::GT => range.1 <= u,
                // max value in column is `< v` so no values can be `>= v`
                cmp::Operator::GTE => range.1 < u,
                // min value in column is `>= v` so no values can be `< v`
                cmp::Operator::LT => range.0 >= u,
                // min value in column is `> v` so no values can be `<= v`
                cmp::Operator::LTE => range.0 > u,
            },
            None => true, // only null values in column so no values satisfy `v`
        }
    }
}

// Converts an Arrow `StringArray` into a `Column`.
impl From<arrow::array::StringArray> for Column {
    fn from(arr: arrow::array::StringArray) -> Self {
        let data = StringEncoding::from(arr);
        let meta = MetaData {
            range: data.column_range(),
            properties: ColumnProperties {
                has_pre_computed_row_ids: data.has_pre_computed_row_id_sets(),
            },
        };

        Self::String(meta, data)
    }
}

impl From<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>> for Column {
    fn from(arr: arrow::array::DictionaryArray<arrow::datatypes::Int32Type>) -> Self {
        let data = StringEncoding::from(arr);
        let meta = MetaData {
            range: data.column_range(),
            properties: ColumnProperties {
                has_pre_computed_row_ids: data.has_pre_computed_row_id_sets(),
            },
        };

        Self::String(meta, data)
    }
}

impl From<&[Option<&str>]> for Column {
    fn from(arr: &[Option<&str>]) -> Self {
        let data = StringEncoding::from(arr);
        let meta = MetaData {
            range: data.column_range(),
            properties: ColumnProperties {
                has_pre_computed_row_ids: data.has_pre_computed_row_id_sets(),
            },
        };

        Self::String(meta, data)
    }
}

impl From<&[Option<String>]> for Column {
    fn from(arr: &[Option<String>]) -> Self {
        Self::from(
            arr.iter()
                .map(|x| x.as_deref())
                .collect::<Vec<Option<&str>>>()
                .as_slice(),
        )
    }
}

impl From<&[&str]> for Column {
    fn from(arr: &[&str]) -> Self {
        let data = StringEncoding::from(arr);
        let meta = MetaData {
            range: data.column_range(),
            properties: ColumnProperties {
                has_pre_computed_row_ids: data.has_pre_computed_row_id_sets(),
            },
        };

        Self::String(meta, data)
    }
}

/// Converts a slice of u64 values into the most compact fixed-width physical
/// encoding.
impl From<&[u64]> for Column {
    fn from(arr: &[u64]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        let data = IntegerEncoding::from(arr);
        let meta = MetaData {
            range: Some((min, max)),
            properties: ColumnProperties::default(),
        };

        Self::Unsigned(meta, data)
    }
}

impl From<arrow::array::UInt64Array> for Column {
    fn from(arr: arrow::array::UInt64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // determine min and max values.
        let mut min: Option<u64> = None;
        let mut max: Option<u64> = None;

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let v = arr.value(i);
            match min {
                Some(m) => {
                    if v < m {
                        min = Some(v);
                    }
                }
                None => min = Some(v),
            };

            match max {
                Some(m) => {
                    if v > m {
                        max = Some(v)
                    }
                }
                None => max = Some(v),
            };
        }

        let range = match (min, max) {
            (None, None) => None,
            (Some(min), Some(max)) => Some((min, max)),
            _ => unreachable!("min/max must both be Some or None"),
        };

        let data = IntegerEncoding::from(arr);
        let meta = MetaData {
            range,
            properties: ColumnProperties::default(),
        };

        Self::Unsigned(meta, data)
    }
}

/// Converts a slice of i64 values into the most compact fixed-width physical
/// encoding.
impl From<&[i64]> for Column {
    fn from(arr: &[i64]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        let data = IntegerEncoding::from(arr);
        let meta = MetaData {
            range: Some((min, max)),
            properties: ColumnProperties::default(),
        };

        Self::Integer(meta, data)
    }
}

impl From<arrow::array::Int64Array> for Column {
    fn from(arr: arrow::array::Int64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // determine min and max values.
        let mut min: Option<i64> = None;
        let mut max: Option<i64> = None;

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let v = arr.value(i);
            match min {
                Some(m) => {
                    if v < m {
                        min = Some(v);
                    }
                }
                None => min = Some(v),
            };

            match max {
                Some(m) => {
                    if v > m {
                        max = Some(v)
                    }
                }
                None => max = Some(v),
            };
        }

        let range = match (min, max) {
            (None, None) => None,
            (Some(min), Some(max)) => Some((min, max)),
            _ => unreachable!("min/max must both be Some or None"),
        };

        let data = IntegerEncoding::from(arr);
        let meta = MetaData {
            range,
            properties: ColumnProperties::default(),
        };

        Self::Integer(meta, data)
    }
}

/// Converts a slice of `f64` values into a fixed-width column encoding.
impl From<&[f64]> for Column {
    fn from(arr: &[f64]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        let data = FloatEncoding::from(arr);
        let meta = MetaData {
            range: Some((min, max)),
            properties: ColumnProperties::default(),
        };

        Self::Float(meta, data)
    }
}

impl From<arrow::array::Float64Array> for Column {
    fn from(arr: arrow::array::Float64Array) -> Self {
        if arr.null_count() == 0 {
            return Self::from(arr.values());
        }

        // determine min and max values.
        let mut min: Option<f64> = None;
        let mut max: Option<f64> = None;

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let v = arr.value(i);
            match min {
                Some(m) => {
                    if v < m {
                        min = Some(v);
                    }
                }
                None => min = Some(v),
            };

            match max {
                Some(m) => {
                    if v > m {
                        max = Some(v)
                    }
                }
                None => max = Some(v),
            };
        }

        let range = match (min, max) {
            (None, None) => None,
            (Some(min), Some(max)) => Some((min, max)),
            _ => unreachable!("min/max must both be Some or None"),
        };

        let data = scalar::FixedNull::<arrow::datatypes::Float64Type>::from(arr);
        let meta = MetaData {
            range,
            ..MetaData::default()
        };

        Self::Float(meta, FloatEncoding::FixedNull64(data))
    }
}

impl From<arrow::array::BooleanArray> for Column {
    fn from(arr: arrow::array::BooleanArray) -> Self {
        // determine min and max values.
        let mut min: Option<bool> = None;
        let mut max: Option<bool> = None;

        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let v = arr.value(i);
            match min {
                Some(m) => {
                    if !v & m {
                        min = Some(v);
                    }
                }
                None => min = Some(v),
            };

            match max {
                Some(m) => {
                    if v & !m {
                        max = Some(v)
                    }
                }
                None => max = Some(v),
            };
        }

        let range = match (min, max) {
            (None, None) => None,
            (Some(min), Some(max)) => Some((min, max)),
            _ => unreachable!("min/max must both be Some or None"),
        };

        let data = BooleanEncoding::from(arr);
        let meta = MetaData {
            range,
            ..MetaData::default()
        };
        Self::Bool(meta, data)
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(_, enc) => enc.fmt(f),
            Self::Float(_, enc) => enc.fmt(f),
            Self::Integer(_, enc) => enc.fmt(f),
            Self::Unsigned(_, enc) => enc.fmt(f),
            Self::Bool(_, enc) => enc.fmt(f),
            Self::ByteArray(_, enc) => enc.fmt(f),
        }
    }
}

#[derive(Debug, PartialEq)]
enum PredicateMatch {
    None,
    SomeMaybe,
    All,
}

/// A specific type of Option for `RowIDs` where the notion of all rows ids is
/// represented.
#[derive(Debug, PartialEq)]
pub enum RowIDsOption {
    None(RowIDs),
    Some(RowIDs),

    // All allows us to indicate to the caller that all possible rows are
    // represented, without having to create a container to store all those ids.
    All(RowIDs),
}

impl RowIDsOption {
    /// Returns the `Some` variant or panics.
    pub fn unwrap(&self) -> &RowIDs {
        match &self {
            Self::None(_) => panic!("cannot unwrap RowIDsOption to RowIDs"),
            Self::Some(ids) => ids,
            Self::All(ids) => ids,
        }
    }
}

/// Represents vectors of row IDs, which are usually used for intermediate
/// results as a method of late materialisation.
#[derive(PartialEq, Debug, Clone)]
pub enum RowIDs {
    Bitmap(Bitmap),
    Vector(Vec<u32>),
}

impl RowIDs {
    pub fn new_bitmap() -> Self {
        Self::Bitmap(Bitmap::create())
    }

    pub fn bitmap_from_slice(arr: &[u32]) -> Self {
        Self::Bitmap(arr.iter().cloned().collect())
    }

    pub fn new_vector() -> Self {
        Self::Vector(vec![])
    }

    pub fn unwrap_bitmap(&self) -> &Bitmap {
        if let Self::Bitmap(bm) = self {
            return bm;
        }
        panic!("cannot unwrap RowIDs to Bitmap");
    }

    pub fn unwrap_vector(&self) -> &Vec<u32> {
        if let Self::Vector(arr) = self {
            return arr;
        }
        panic!("cannot unwrap RowIDs to Vector");
    }

    /// An estimation of the size in bytes needed to store `self`.
    pub fn size(&self) -> usize {
        match self {
            Self::Bitmap(bm) => std::mem::size_of::<Bitmap>() + bm.get_serialized_size_in_bytes(),
            Self::Vector(v) => {
                std::mem::size_of::<Vec<u32>>() + (std::mem::size_of::<u32>() * v.len())
            }
        }
    }

    /// Returns an iterator over the contents of the RowIDs.
    pub fn iter(&self) -> RowIDsIterator<'_> {
        match self {
            Self::Bitmap(bm) => RowIDsIterator::new(bm.iter()),
            // we want an iterator of u32 rather than &u32.
            Self::Vector(vec) => RowIDsIterator::new(vec.iter().cloned()),
        }
    }

    // Converts the RowIDs to a Vec<u32>. This is expensive and should only be
    // used for testing.
    pub fn to_vec(&self) -> Vec<u32> {
        match self {
            Self::Bitmap(bm) => bm.to_vec(),
            Self::Vector(arr) => arr.clone(),
        }
    }

    pub fn as_slice(&self) -> &[u32] {
        match self {
            Self::Bitmap(_) => panic!("as_slice not supported on bitmap variant"),
            Self::Vector(arr) => arr.as_slice(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Bitmap(ids) => ids.cardinality() as usize,
            Self::Vector(ids) => ids.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Bitmap(ids) => ids.is_empty(),
            Self::Vector(ids) => ids.is_empty(),
        }
    }

    pub fn clear(&mut self) {
        match self {
            Self::Bitmap(ids) => ids.clear(),
            Self::Vector(ids) => ids.clear(),
        }
    }

    pub fn add(&mut self, id: u32) {
        match self {
            Self::Bitmap(ids) => ids.add(id),
            Self::Vector(ids) => ids.push(id),
        }
    }

    pub fn add_range(&mut self, from: u32, to: u32) {
        match self {
            Self::Bitmap(ids) => ids.add_range(from as u64..to as u64),
            Self::Vector(ids) => ids.extend(from..to),
        }
    }

    // Adds all the values from the provided bitmap into self.
    pub fn add_from_bitmap(&mut self, other: &croaring::Bitmap) {
        match self {
            Self::Bitmap(_self) => _self.or_inplace(other),
            Self::Vector(_self) => _self.extend_from_slice(other.to_vec().as_slice()),
        }
    }

    pub fn intersect(&mut self, other: &Self) {
        match (self, other) {
            (Self::Bitmap(_self), Self::Bitmap(ref other)) => _self.and_inplace(other),
            (_, _) => unimplemented!("currently unsupported"),
        };
    }

    pub fn union(&mut self, other: &Self) {
        match (self, other) {
            (Self::Bitmap(inner), Self::Bitmap(other)) => inner.or_inplace(other),
            // N.B this seems very inefficient. It should only be used for testing.
            (Self::Vector(inner), Self::Bitmap(other)) => {
                let mut bm: Bitmap = inner.iter().cloned().collect();
                bm.or_inplace(other);

                inner.clear();
                inner.extend(bm.iter());
            }
            (_, _) => unimplemented!("currently unsupported"),
        };
    }
}

pub struct RowIDsIterator<'a> {
    itr: Box<dyn Iterator<Item = u32> + 'a>,
}

impl<'a> RowIDsIterator<'a> {
    fn new(itr: impl Iterator<Item = u32> + 'a) -> Self {
        Self { itr: Box::new(itr) }
    }
}

impl Iterator for RowIDsIterator<'_> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.itr.next()
    }
}

// Statistics about the composition of a column
pub(crate) struct Statistics {
    pub enc_type: &'static str,
    pub log_data_type: &'static str,
    pub values: u32,
    pub nulls: u32,
    pub bytes: usize,
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use encoding::string::NULL_ID;

    #[test]
    fn row_ids_intersect() {
        let mut row_ids = RowIDs::new_bitmap();
        row_ids.add_range(0, 5);

        let mut other = RowIDs::new_bitmap();
        other.add_range(2, 7);

        row_ids.intersect(&other);
        assert_eq!(row_ids.to_vec(), vec![2, 3, 4]);
    }

    #[test]
    fn from_arrow_string_array_column_meta() {
        let cases = vec![
            (
                StringArray::from(vec![None, Some("world"), None, Some("hello")]),
                (
                    OwnedValue::String("hello".to_owned()),
                    OwnedValue::String("world".to_owned()),
                ),
            ),
            (
                StringArray::from(vec![None, Some("world"), None]),
                (
                    OwnedValue::String("world".to_owned()),
                    OwnedValue::String("world".to_owned()),
                ),
            ),
            (
                StringArray::from(vec![None, None]),
                (OwnedValue::Null, OwnedValue::Null),
            ),
        ];

        for (arr, range) in cases {
            let col = Column::from(arr);
            assert_eq!(col.column_range(), range);
        }
    }

    #[test]
    fn from_arrow_int_array_column_meta() {
        let cases = vec![
            (
                Int64Array::from(vec![None, Some(22), None, Some(18)]),
                (
                    OwnedValue::Scalar(Scalar::I64(18)),
                    OwnedValue::Scalar(Scalar::I64(22)),
                ),
            ),
            (
                Int64Array::from(vec![None, Some(22), None]),
                (
                    OwnedValue::Scalar(Scalar::I64(22)),
                    OwnedValue::Scalar(Scalar::I64(22)),
                ),
            ),
            (
                Int64Array::from(vec![None, None]),
                (OwnedValue::Null, OwnedValue::Null),
            ),
        ];

        for (arr, range) in cases {
            let col = Column::from(arr);
            assert_eq!(col.column_range(), range);
        }
    }

    #[test]
    fn from_arrow_string_array() {
        let input = vec![None, Some("world"), None, Some("hello")];
        let arr = StringArray::from(input);

        let col = Column::from(arr);
        if let Column::String(meta, StringEncoding::RleDictionary(enc)) = col {
            assert_eq!(
                meta,
                super::MetaData::<String> {
                    range: Some(("hello".to_string(), "world".to_string())),
                    properties: ColumnProperties {
                        has_pre_computed_row_ids: true
                    }
                }
            );

            assert_eq!(
                enc.all_values(vec![]),
                vec![None, Some("world"), None, Some("hello")]
            );

            assert_eq!(enc.all_encoded_values(vec![]), vec![0, 2, 0, 1,]);
        } else {
            panic!("invalid type");
        }
    }

    #[test]
    fn from_strs() {
        let arr = vec!["world", "hello"];
        let col = Column::from(arr.as_slice());
        if let Column::String(meta, StringEncoding::RleDictionary(enc)) = col {
            assert_eq!(
                meta,
                super::MetaData::<String> {
                    range: Some(("hello".to_string(), "world".to_string())),
                    properties: ColumnProperties {
                        has_pre_computed_row_ids: true
                    }
                }
            );

            assert_eq!(enc.all_values(vec![]), vec![Some("world"), Some("hello")]);

            assert_eq!(enc.all_encoded_values(vec![]), vec![2, 1]);
        } else {
            panic!("invalid type");
        }
    }

    #[test]
    fn from_i64_slice() {
        let input = &[-1, i8::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64I8(_))
        ));

        let input = &[0, u8::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64U8(_))
        ));

        let input = &[-1, i16::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64I16(_))
        ));

        let input = &[0, u16::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64U16(_))
        ));

        let input = &[-1, i32::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64I32(_))
        ));

        let input = &[0, u32::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64U32(_))
        ));

        let input = &[-1, i64::MAX];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64I64(_))
        ));

        // validate min/max check
        let input = &[0, -12, u16::MAX as i64, 5];
        let col = Column::from(&input[..]);
        if let Column::Integer(meta, IntegerEncoding::I64I32(_)) = col {
            assert_eq!(meta.range, Some((-12, u16::MAX as i64)));
        } else {
            panic!("invalid variant");
        }
    }

    #[test]
    fn from_u64_slice() {
        let input = &[0, u8::MAX as u64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U64U8(_))
        ));

        let input = &[0, u16::MAX as u64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U64U16(_))
        ));

        let input = &[0, u32::MAX as u64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U64U32(_))
        ));

        // validate min/max check
        let input = &[13, 12, u16::MAX as u64, 5];
        let col = Column::from(&input[..]);
        if let Column::Unsigned(meta, IntegerEncoding::U64U16(_)) = col {
            assert_eq!(meta.range, Some((5, u16::MAX as u64)));
        } else {
            panic!("invalid variant");
        }
    }

    #[test]
    fn value() {
        // The Scalar variant always represents the logical type of the column.
        // The `value` method always returns values according to the logical
        // type, no matter what the underlying physical type might be.

        // `col` will stored as `i16`, but logical type in and out is `i64`.
        let col = Column::from(&[0_i64, 1, 200, 20, -1][..]);
        assert_eq!(col.value(4), Value::from(-1_i64));

        // `col` will stored as `u16`, but logical type in and out is `u64`.
        let col = Column::from(&[20_u64, 300][..]);
        assert_eq!(col.value(1), Value::from(300_u64));

        // `col` will stored as `u8`, but logical type in and out is `u64`.
        let col = Column::from(&[20_u64, 3][..]);
        assert_eq!(col.value(0), Value::from(20_u64));

        // `col` will stored as `u8`, but logical type in and out is `u64`.
        let col = Column::from(&[20_u64, 3][..]);
        assert_eq!(col.value(1), Value::from(3_u64));

        // `col` will stored as `u8`, but logical type in and out is `u64`.
        let col = Column::from(&[243_u64, 198][..]);
        assert_eq!(col.value(0), Value::from(243_u64));

        let col = Column::from(&[-19.2, -30.2][..]);
        assert_eq!(col.value(0), Value::from(-19.2));

        let col = Column::from(&[Some("a"), Some("b"), None, Some("c")][..]);
        assert_eq!(col.value(1), Value::from("b"));
        assert_eq!(col.value(2), Value::Null);
    }

    #[test]
    fn values() {
        // `col` will stored as `i16`, but logical type in and out is `i64`.
        let col = Column::from(&[0_i64, 1, 200, 20, -1][..]);
        assert_eq!(col.values(&[0, 2, 3]), Values::I64(vec![0, 200, 20]));

        // `col` will stored as `i16`, but logical type in and out is `i64`.
        let col = Column::from(&[0_i64, 1, 200, 20, -1][..]);
        assert_eq!(col.values(&[0, 2, 3]), Values::I64(vec![0, 200, 20]));

        // `col` will stored as `i16`, but logical type in and out is `i64`.
        let col = Column::from(&[0_i64, 1, 200, 20, -1][..]);
        assert_eq!(col.values(&[0, 2, 3]), Values::I64(vec![0, 200, 20]));

        // `col` will stored as `i8`, but logical type in and out is `i64`.
        let col = Column::from(&[0_i64, 1, 127, 20, -1][..]);
        assert_eq!(col.values(&[0, 2, 3]), Values::I64(vec![0, 127, 20]));

        // `col` will stored as `u8`, but logical type in and out is `u64`.
        let col = Column::from(&[0_u64, 1, 200, 20, 100][..]);
        assert_eq!(col.values(&[3, 4]), Values::U64(vec![20, 100]));

        // `col` will stored as `u8`, but logical type in and out is `u64`.
        let col = Column::from(&[0_u64, 1, 200, 20, 100][..]);
        assert_eq!(col.values(&[3, 4]), Values::U64(vec![20, 100]));

        // `col` will stored as `u8`, but logical type in and out is `u64`.
        let col = Column::from(&[0_u64, 1, 200, 20, 100][..]);
        assert_eq!(col.values(&[3, 4]), Values::U64(vec![20, 100]));

        // `col` will stored as `u8`, but logical type in and out is `u64`.
        let col = Column::from(&[0_u64, 1, 200, 20, 100][..]);
        assert_eq!(col.values(&[3, 4]), Values::U64(vec![20, 100]));

        // physical and logical type of `col` will be `f64`
        let col = Column::from(&[0.0, 1.1, 20.2, 22.3, 100.1324][..]);
        assert_eq!(col.values(&[1, 3]), Values::F64(vec![1.1, 22.3]));

        let col = Column::from(&[Some("a"), Some("b"), None, Some("c")][..]);
        assert_eq!(
            col.values(&[1, 2, 3]),
            Values::String(vec![Some("b"), None, Some("c")])
        );
    }

    #[test]
    fn distinct_values() {
        let input = &[
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            Some("world"),
        ];

        let mut exp = BTreeSet::new();
        exp.insert(Some("hello"));
        exp.insert(Some("world"));
        exp.insert(None);

        let col = Column::from(&input[..]);
        assert_eq!(col.distinct_values(vec![0, 1, 2, 3, 4].into_iter()), exp);
        assert_eq!(
            col.distinct_values(RowIDs::Vector(vec![0, 1, 2, 3, 4]).iter()),
            exp
        );

        let mut bm = Bitmap::create();
        bm.add_many(&[0, 1, 2, 3, 4]);
        assert_eq!(col.distinct_values(RowIDs::Bitmap(bm).iter()), exp);
    }

    #[test]
    fn encoded_values() {
        let input = &[
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            Some("world"),
        ];

        let col = Column::from(&input[..]);
        assert_eq!(
            col.encoded_values(&[0, 1, 2, 3, 4], EncodedValues::U32(vec![])),
            EncodedValues::U32(vec![1, NULL_ID, 2, 1, 2])
        );

        let res = col.encoded_values(&[2, 3], EncodedValues::U32(Vec::with_capacity(100)));
        assert_eq!(res, EncodedValues::U32(vec![2, 1]));
        if let EncodedValues::U32(v) = res {
            assert_eq!(v.capacity(), 100);
        } else {
            panic!("not a EncodedValues::U32")
        }

        // timestamp column
        let input = &[
            1001231231243_i64,
            1001231231343,
            1001231231443,
            1001231231543,
        ];
        let col = Column::from(&input[..]);
        assert_eq!(
            col.encoded_values(&[0, 2], EncodedValues::I64(vec![])),
            EncodedValues::I64(vec![1001231231243, 1001231231443])
        );
    }

    #[test]
    fn all_encoded_values() {
        let input = &[
            Some("hello"),
            None,
            Some("world"),
            Some("hello"),
            Some("world"),
        ];

        let col = Column::from(&input[..]);
        assert_eq!(
            col.all_encoded_values(EncodedValues::U32(vec![])),
            EncodedValues::U32(vec![1, NULL_ID, 2, 1, 2])
        );

        // timestamp column
        let input = &[
            1001231231243_i64,
            1001231231343,
            1001231231443,
            1001231231543,
        ];
        let col = Column::from(&input[..]);
        assert_eq!(
            col.all_encoded_values(EncodedValues::I64(vec![])),
            EncodedValues::I64(vec![
                1001231231243,
                1001231231343,
                1001231231443,
                1001231231543,
            ])
        );
    }

    #[test]
    fn row_ids_filter_str() {
        let input = &[
            Some("Badlands"),
            None,
            Some("Racing in the Street"),
            Some("Streets of Fire"),
            None,
            None,
            Some("Darkness on the Edge of Town"),
        ];

        // re-use the buffer to ensure it's cleared correctly between calls.
        let mut dst_buffer = RowIDs::new_bitmap();

        let col = Column::from(&input[..]);
        let mut row_ids =
            col.row_ids_filter(&cmp::Operator::Equal, &Value::from("Badlands"), dst_buffer);
        match row_ids {
            RowIDsOption::None(_) => panic!("expected some rows"),
            RowIDsOption::Some(dst) => {
                assert_eq!(dst.to_vec(), vec![0]);
                dst_buffer = dst;
            }
            RowIDsOption::All(_) => panic!("expected some rows"),
        }

        row_ids = col.row_ids_filter(&cmp::Operator::Equal, &Value::from("Factory"), dst_buffer);
        match row_ids {
            RowIDsOption::None(_dst) => {
                dst_buffer = _dst;
            }
            RowIDsOption::Some(_) => panic!("expected no rows"),
            RowIDsOption::All(_) => panic!("expected no rows"),
        }

        row_ids = col.row_ids_filter(
            &cmp::Operator::GT,
            &Value::from("Adam Raised a Cain"),
            dst_buffer,
        );
        match row_ids {
            RowIDsOption::None(_) => panic!("expected some rows"),
            RowIDsOption::Some(_dst) => {
                assert_eq!(_dst.to_vec(), vec![0, 2, 3, 6]);
                dst_buffer = _dst;
            }
            RowIDsOption::All(_) => panic!("expected some rows"),
        }

        row_ids = col.row_ids_filter(
            &cmp::Operator::LTE,
            &Value::from("Streets of Fire"),
            dst_buffer,
        );
        match row_ids {
            RowIDsOption::None(_) => panic!("expected some rows"),
            RowIDsOption::Some(_dst) => {
                assert_eq!(_dst.to_vec(), vec![0, 2, 3, 6]);
                dst_buffer = _dst;
            }
            RowIDsOption::All(_) => panic!("expected some rows"),
        }

        row_ids = col.row_ids_filter(
            &cmp::Operator::LT,
            &Value::from("Something in the Night"),
            dst_buffer,
        );
        match row_ids {
            RowIDsOption::None(_) => panic!("expected some rows"),
            RowIDsOption::Some(_dst) => {
                assert_eq!(_dst.to_vec(), vec![0, 2, 6]);
                dst_buffer = _dst;
            }
            RowIDsOption::All(_) => panic!("expected some rows"),
        }

        // when the column doesn't contain any NULL values the `All` variant
        // might be returned.
        let input = &[
            Some("Badlands"),
            Some("Racing in the Street"),
            Some("Streets of Fire"),
            Some("Darkness on the Edge of Town"),
        ];

        let col = Column::from(&input[..]);
        row_ids = col.row_ids_filter(
            &cmp::Operator::NotEqual,
            &Value::from("Adam Raised a Cain"),
            dst_buffer,
        );
        match row_ids {
            RowIDsOption::None(_dst) => panic!("expected all rows"),
            RowIDsOption::Some(_) => panic!("expected no rows"),
            RowIDsOption::All(_dst) => {
                dst_buffer = _dst;
            }
        }

        row_ids = col.row_ids_filter(
            &cmp::Operator::GT,
            &Value::from("Adam Raised a Cain"),
            dst_buffer,
        );
        match row_ids {
            RowIDsOption::None(_dst) => panic!("expected all rows"),
            RowIDsOption::Some(_) => panic!("expected no rows"),
            RowIDsOption::All(_dst) => {
                dst_buffer = _dst;
            }
        }

        row_ids = col.row_ids_filter(
            &cmp::Operator::NotEqual,
            &Value::from("Thunder Road"),
            dst_buffer,
        );
        match row_ids {
            RowIDsOption::None(_dst) => panic!("expected all rows"),
            RowIDsOption::Some(_) => panic!("expected no rows"),
            RowIDsOption::All(_dst) => {
                dst_buffer = _dst;
            }
        }
        // This buffer's contents is from the last time it was populated, which
        // was the `< "Something in the Night"` predicate above.
        assert_eq!(dst_buffer.to_vec(), vec![0, 2, 6]);
    }

    #[test]
    fn row_ids_filter_int() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];

        let col = Column::from(&input[..]);
        let mut row_ids = col.row_ids_filter(
            &cmp::Operator::Equal,
            &Value::from(200_i64),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 4]);

        row_ids = col.row_ids_filter(
            &cmp::Operator::Equal,
            &Value::from(2000_i64),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        row_ids = col.row_ids_filter(
            &cmp::Operator::GT,
            &Value::from(2_i64),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 4, 5, 6]);

        row_ids = col.row_ids_filter(
            &cmp::Operator::GTE,
            &Value::from(2_u64),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        row_ids = col.row_ids_filter(
            &cmp::Operator::NotEqual,
            &Value::from(-1257_i64),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        row_ids = col.row_ids_filter(
            &cmp::Operator::LT,
            &Value::from(i64::MAX),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        let input = vec![
            Some(100_i64),
            Some(200),
            None,
            None,
            Some(200),
            Some(22),
            Some(30),
        ];
        let arr = Int64Array::from(input);
        let col = Column::from(arr);
        row_ids = col.row_ids_filter(
            &cmp::Operator::GT,
            &Value::from(10_i64),
            RowIDs::new_vector(), // exercise alternative row ids representation
        );
        assert_eq!(row_ids.unwrap().as_slice(), &[0, 1, 4, 5, 6]);
    }

    #[test]
    fn row_ids_filter_uint() {
        let input = &[100_u64, 200, 300, 2, 200, 22, 30];

        let col = Column::from(&input[..]);
        let mut row_ids = col.row_ids_filter(
            &cmp::Operator::Equal,
            &Value::from(200_i64),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 4]);

        row_ids = col.row_ids_filter(
            &cmp::Operator::Equal,
            &Value::from(2000_u64),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        row_ids = col.row_ids_filter(
            &cmp::Operator::GT,
            &Value::from(2_i64),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 4, 5, 6]);

        row_ids = col.row_ids_filter(
            &cmp::Operator::GTE,
            &Value::from(2_i64),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        row_ids = col.row_ids_filter(
            &cmp::Operator::NotEqual,
            &Value::from(-1257_i64),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));
    }

    #[test]
    fn row_ids_filter_float() {
        let input = &[100.2, 200.0, 300.1, 2.22, -200.2, 22.2, 30.2];

        let col = Column::from(&input[..]);
        let mut row_ids = col.row_ids_filter(
            &cmp::Operator::Equal,
            &Value::from(200.0),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![1]);

        row_ids = col.row_ids_filter(
            &cmp::Operator::Equal,
            &Value::from(2000.0),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        row_ids = col.row_ids_filter(
            &cmp::Operator::GT,
            &Value::from(-200.0),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 3, 5, 6]);

        row_ids = col.row_ids_filter(
            &cmp::Operator::GTE,
            &Value::from(-200.2),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        row_ids = col.row_ids_filter(
            &cmp::Operator::NotEqual,
            &Value::from(-1257.029),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));
    }

    #[test]
    fn row_ids_range() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];

        let col = Column::from(&input[..]);
        let mut row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GT, Value::from(100_i64)),
            &(cmp::Operator::LT, Value::from(300_i64)),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 4]);

        row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GTE, Value::from(200_i64)),
            &(cmp::Operator::LTE, Value::from(300_i64)),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 2, 4]);

        row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GTE, Value::from(23333_u64)),
            &(cmp::Operator::LTE, Value::from(999999_u64)),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::None(_)));

        row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GT, Value::from(-100_i64)),
            &(cmp::Operator::LT, Value::from(301_i64)),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GTE, Value::from(2_i64)),
            &(cmp::Operator::LTE, Value::from(300_i64)),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GTE, Value::from(87_i64)),
            &(cmp::Operator::LTE, Value::from(999999_i64)),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 4]);

        row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GTE, Value::from(0_i64)),
            &(cmp::Operator::NotEqual, Value::from(i64::MAX)),
            RowIDs::new_bitmap(),
        );
        assert!(matches!(row_ids, RowIDsOption::All(_)));

        row_ids = col.row_ids_filter_range(
            &(cmp::Operator::GTE, Value::from(0_i64)),
            &(cmp::Operator::NotEqual, Value::from(99_i64)),
            RowIDs::new_bitmap(),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn might_contain_value() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30, -1228282828282];
        let col = Column::from(&input[..]);
        assert!(matches!(
            col,
            Column::Integer(_, IntegerEncoding::I64I64(_))
        ));

        let cases: Vec<(Scalar, bool)> = vec![
            (Scalar::I64(200), true),
            (Scalar::U64(200), true),
            (Scalar::I64(100), true),
            (Scalar::I64(30), true),
            (Scalar::U64(2), true),
            (Scalar::U64(100000000), false),
            (Scalar::I64(-9228282828282), false),
            (Scalar::U64(u64::MAX), false),
        ];

        for (scalar, result) in cases.clone() {
            assert_eq!(col.might_contain_value(&Value::Scalar(scalar)), result);
        }

        // Input stored as different physical size
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert!(matches!(
            col,
            Column::Integer(_, IntegerEncoding::I64U16(_))
        ));

        for (scalar, result) in cases.clone() {
            assert_eq!(col.might_contain_value(&Value::Scalar(scalar)), result);
        }

        // Input stored as unsigned column
        let input = &[100_u64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert!(matches!(
            col,
            Column::Unsigned(_, IntegerEncoding::U64U16(_))
        ));

        for (scalar, result) in cases.clone() {
            assert_eq!(col.might_contain_value(&Value::Scalar(scalar)), result);
        }

        let input = &[100.0, 200.2, 300.2];
        let col = Column::from(&input[..]);

        let cases: Vec<(Scalar, bool)> = vec![
            (Scalar::F64(100.0), true),
            (Scalar::F64(200.2), true),
            (Scalar::F64(-100.0), false),
        ];

        for (scalar, result) in cases.clone() {
            assert_eq!(col.might_contain_value(&Value::Scalar(scalar)), result);
        }
    }

    #[test]
    fn predicate_matches_all_values() {
        let input = &[100i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);

        let cases: Vec<(cmp::Operator, Scalar, bool)> = vec![
            (cmp::Operator::GT, Scalar::U64(100), false),
            (cmp::Operator::GT, Scalar::I64(100), false),
            (cmp::Operator::GT, Scalar::I64(-99), true),
            (cmp::Operator::GT, Scalar::I64(100), false),
            (cmp::Operator::LT, Scalar::I64(300), false),
            (cmp::Operator::Equal, Scalar::U64(2), false),
            (cmp::Operator::NotEqual, Scalar::I64(1), true),
            (cmp::Operator::NotEqual, Scalar::I64(301), true),
        ];

        for (op, scalar, result) in cases {
            assert_eq!(
                col.predicate_matches_all_values(&op, &Value::Scalar(scalar)),
                result,
            );
        }

        // Future improvement would be to support this type of check.
        let input = &[100_i64, -20];
        let col = Column::from(&input[..]);
        assert_eq!(
            col.predicate_matches_all_values(&cmp::Operator::LT, &Value::from(u64::MAX)),
            false
        );
    }

    #[test]
    fn evaluate_predicate_on_meta() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);

        let cases: Vec<(cmp::Operator, Scalar, PredicateMatch)> = vec![
            (
                cmp::Operator::GT,
                Scalar::U64(100),
                PredicateMatch::SomeMaybe,
            ),
            (
                cmp::Operator::GT,
                Scalar::I64(100),
                PredicateMatch::SomeMaybe,
            ),
            (cmp::Operator::GT, Scalar::I64(-99), PredicateMatch::All),
            (
                cmp::Operator::GT,
                Scalar::I64(100),
                PredicateMatch::SomeMaybe,
            ),
            (
                cmp::Operator::LT,
                Scalar::I64(300),
                PredicateMatch::SomeMaybe,
            ),
            (cmp::Operator::LTE, Scalar::I64(300), PredicateMatch::All),
            (
                cmp::Operator::Equal,
                Scalar::I64(2),
                PredicateMatch::SomeMaybe,
            ),
            (
                cmp::Operator::NotEqual,
                Scalar::I64(2),
                PredicateMatch::SomeMaybe,
            ),
            (cmp::Operator::NotEqual, Scalar::I64(1), PredicateMatch::All),
            (
                cmp::Operator::NotEqual,
                Scalar::I64(301),
                PredicateMatch::All,
            ),
            (cmp::Operator::GT, Scalar::I64(100000), PredicateMatch::None),
            (cmp::Operator::GTE, Scalar::I64(301), PredicateMatch::None),
            (cmp::Operator::LT, Scalar::I64(2), PredicateMatch::None),
            (cmp::Operator::LTE, Scalar::I64(-100), PredicateMatch::None),
            (
                cmp::Operator::Equal,
                Scalar::I64(100000),
                PredicateMatch::None,
            ),
        ];

        for (op, scalar, result) in cases {
            assert_eq!(
                col.evaluate_predicate_on_meta(&op, &Value::Scalar(scalar)),
                result
            );
        }
    }

    #[test]
    fn min() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.min(&[0, 1, 3][..]), Value::from(2_i64));
        assert_eq!(col.min(&[0, 1, 2][..]), Value::from(100_i64));

        let input = &[100_u64, 200, 245, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.min(&[4, 6][..]), Value::from(30_u64));

        let input = &[Some("hello"), None, Some("world")];
        let col = Column::from(&input[..]);
        assert_eq!(col.min(&[0, 1, 2][..]), Value::from("hello"));
        assert_eq!(col.min(&[1][..]), Value::Null);
    }

    #[test]
    fn max() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.max(&[0, 1, 3][..]), Value::from(200_i64));
        assert_eq!(col.max(&[0, 1, 2][..]), Value::from(300_i64));

        let input = &[10.2_f64, -2.43, 200.2];
        let col = Column::from(&input[..]);
        assert_eq!(col.max(&[0, 1, 2][..]), Value::from(200.2));

        let input = vec![None, Some(200), None];
        let arr = Int64Array::from(input);
        let col = Column::from(arr);
        assert_eq!(col.max(&[0, 1, 2][..]), Value::from(200_i64));

        let input = &[Some("hello"), None, Some("world")];
        let col = Column::from(&input[..]);
        assert_eq!(col.max(&[0, 1, 2][..]), Value::from("world"));
        assert_eq!(col.max(&[1][..]), Value::Null);
    }

    #[test]
    fn sum() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.sum(&[0, 1, 3][..]), Scalar::I64(302));
        assert_eq!(col.sum(&[0, 1, 2][..]), Scalar::I64(600));

        let input = &[10.2_f64, -2.43, 200.2];
        let col = Column::from(&input[..]);
        assert_eq!(col.sum(&[0, 1, 2][..]), Scalar::F64(207.97));

        let input = vec![None, Some(200), None];
        let arr = Int64Array::from(input);
        let col = Column::from(arr);
        assert_eq!(col.sum(&[0, 1, 2][..]), Scalar::I64(200));
    }

    #[test]
    fn count() {
        let input = &[100_i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.count(&[0, 1, 3][..]), 3);

        let input = &[10.2_f64, -2.43, 200.2];
        let col = Column::from(&input[..]);
        assert_eq!(col.count(&[0, 1][..]), 2);

        let input = vec![None, Some(200), None];
        let arr = Int64Array::from(input);
        let col = Column::from(arr);
        assert_eq!(col.count(&[0, 1, 2][..]), 1);
        assert_eq!(col.count(&[0, 2][..]), 0);
    }

    #[test]
    fn has_non_null_value() {
        // Check each column type is wired up. Actual logic is tested in encoders.

        let col = Column::from(&[Some("Knocked Down"), None][..]);
        assert!(col.has_non_null_value(&[0, 1]));

        let col = Column::from(&[100_u64][..]);
        assert!(col.has_non_null_value(&[0]));

        let col = Column::from(&[100_i64][..]);
        assert!(col.has_non_null_value(&[0]));

        let col = Column::from(&[22.6][..]);
        assert!(col.has_non_null_value(&[0]));

        let col = Column::from(arrow::array::BooleanArray::from(vec![true]));
        assert!(col.has_non_null_value(&[0]));
    }
}
