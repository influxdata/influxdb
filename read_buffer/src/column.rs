pub mod boolean;
pub mod cmp;
pub mod encoding;
pub mod float;
pub mod integer;

use std::collections::BTreeSet;
use std::convert::TryFrom;
use std::sync::Arc;

use arrow::array;
use croaring::Bitmap;
use either::Either;

use arrow_deps::{arrow, arrow::array::Array};

use crate::schema::{AggregateType, LogicalDataType};
use boolean::BooleanEncoding;
use encoding::{bool, dictionary, fixed, fixed_null};
use float::FloatEncoding;
use integer::IntegerEncoding;

// Edd's totally made up magic constant. This determines whether we would use
// a run-length encoded dictionary encoding or just a plain dictionary encoding.
// I have ideas about how to build heuristics to do this in a much better way
// than an arbitrary constant but for now it's this...
//
// FWIW it's not the cardinality of the column that should drive the decision
// it's how many run-lengths would be produced in an RLE column and whether that
// compression is worth the memory and compute costs to work on it.
pub const TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT: usize = 100_000;

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
    pub fn num_rows(&self) -> u32 {
        match &self {
            Column::String(meta, _) => meta.rows,
            Column::Float(meta, _) => meta.rows,
            Column::Integer(meta, _) => meta.rows,
            Column::Unsigned(meta, _) => meta.rows,
            Column::Bool(meta, _) => meta.rows,
            Column::ByteArray(meta, _) => meta.rows,
        }
    }

    /// Returns the logical data-type associated with the column.
    pub fn logical_datatype(&self) -> LogicalDataType {
        match self {
            Column::String(_, _) => LogicalDataType::String,
            Column::Float(_, _) => LogicalDataType::Float,
            Column::Integer(_, _) => LogicalDataType::Integer,
            Column::Unsigned(_, _) => LogicalDataType::Unsigned,
            Column::Bool(_, _) => LogicalDataType::Boolean,
            Column::ByteArray(_, _) => LogicalDataType::Binary,
        }
    }

    pub fn size(&self) -> u64 {
        0
    }

    /// Returns the (min, max)  values stored in this column
    pub fn column_range(&self) -> Option<(OwnedValue, OwnedValue)> {
        match &self {
            Column::String(meta, _) => match &meta.range {
                Some(range) => Some((
                    OwnedValue::String(range.0.clone()),
                    OwnedValue::String(range.1.clone()),
                )),
                None => None,
            },
            Column::Float(meta, _) => match meta.range {
                Some(range) => Some((
                    OwnedValue::Scalar(Scalar::F64(range.0)),
                    OwnedValue::Scalar(Scalar::F64(range.1)),
                )),
                None => None,
            },
            Column::Integer(meta, _) => match meta.range {
                Some(range) => Some((
                    OwnedValue::Scalar(Scalar::I64(range.0)),
                    OwnedValue::Scalar(Scalar::I64(range.1)),
                )),
                None => None,
            },
            Column::Unsigned(meta, _) => match meta.range {
                Some(range) => Some((
                    OwnedValue::Scalar(Scalar::U64(range.0)),
                    OwnedValue::Scalar(Scalar::U64(range.1)),
                )),
                None => None,
            },
            Column::Bool(meta, _) => match meta.range {
                Some(range) => Some((OwnedValue::Boolean(range.0), OwnedValue::Boolean(range.1))),
                None => None,
            },
            Column::ByteArray(_, _) => todo!(),
        }
    }

    pub fn properties(&self) -> &ColumnProperties {
        match &self {
            Column::String(meta, _) => &meta.properties,
            Column::Float(meta, _) => &meta.properties,
            Column::Integer(meta, _) => &meta.properties,
            Column::Unsigned(meta, _) => &meta.properties,
            Column::Bool(meta, _) => &meta.properties,
            Column::ByteArray(meta, _) => &meta.properties,
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
            Column::String(_, data) => data.value(row_id),
            Column::Float(_, data) => data.value(row_id),
            Column::Integer(_, data) => data.value(row_id),
            Column::Unsigned(_, data) => data.value(row_id),
            Column::Bool(_, data) => data.value(row_id),
            Column::ByteArray(_, _) => todo!(),
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
            Column::String(_, data) => data.values(row_ids),
            Column::Float(_, data) => data.values(row_ids),
            Column::Integer(_, data) => data.values(row_ids),
            Column::Unsigned(_, data) => data.values(row_ids),
            Column::Bool(_, data) => data.values(row_ids),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    /// All logical values in the column.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Column::String(_, data) => data.all_values(),
            Column::Float(_, data) => data.all_values(),
            Column::Integer(_, data) => data.all_values(),
            Column::Unsigned(_, data) => data.all_values(),
            Column::Bool(_, data) => data.all_values(),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    /// The value present at the provided logical row id.
    pub fn decode_id(&self, encoded_id: u32) -> Value<'_> {
        match &self {
            Column::String(_, data) => data.decode_id(encoded_id),
            Column::ByteArray(_, _) => todo!(),
            _ => panic!("unsupported operation"),
        }
    }

    // The distinct set of values found at the logical row ids.
    pub fn distinct_values(&self, row_ids: &[u32]) -> ValueSet<'_> {
        assert!(
            row_ids.len() as u32 <= self.num_rows(),
            "too many row ids {:?} provided for column with {:?} rows",
            row_ids.len(),
            self.num_rows()
        );

        match &self {
            Column::String(_, data) => data.distinct_values(row_ids),
            Column::ByteArray(_, _) => todo!(),
            _ => unimplemented!("distinct values is not implemented for this type"),
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
            Column::String(_, data) => data.group_row_ids(),
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
            Column::String(_, data) => data.row_ids_filter(op, value.string(), dst),
            Column::Float(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Column::Integer(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Column::Unsigned(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Column::Bool(_, data) => data.row_ids_filter(op, value.bool(), dst),
            Column::ByteArray(_, data) => todo!(),
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
            Column::String(_, data) => unimplemented!("not supported on string columns yet"),
            Column::Float(_, data) => data.row_ids_filter_range(low_scalar, high_scalar, dst),
            Column::Integer(_, data) => data.row_ids_filter_range(low_scalar, high_scalar, dst),
            Column::Unsigned(_, data) => {
                data.row_ids_filter_range((&low.0, low.1.scalar()), (&high.0, high.1.scalar()), dst)
            }
            Column::Bool(_, data) => unimplemented!("filter_range not supported on boolean column"),
            Column::ByteArray(_, data) => todo!(),
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
            Column::String(meta, _) => {
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
            Column::Float(meta, _) => value
                .scalar()
                .try_as_f64()
                .map_or_else(|| false, |v| meta.might_contain_value(v)),
            Column::Integer(meta, _) => value
                .scalar()
                .try_as_i64()
                .map_or_else(|| false, |v| meta.might_contain_value(v)),
            Column::Unsigned(meta, _) => value
                .scalar()
                .try_as_u64()
                .map_or_else(|| false, |v| meta.might_contain_value(v)),
            Column::Bool(meta, _) => match value {
                Value::Null => false,
                Value::Boolean(b) => meta.might_contain_value(*b),
                v => panic!("cannot compare boolean to {:?}", v),
            },
            Column::ByteArray(meta, _) => todo!(),
        }
    }

    // Helper method to determine if the predicate matches all the values in
    // the column.
    //
    // TODO(edd): this doesn't handle operators that compare to a NULL value yet.
    fn predicate_matches_all_values(&self, op: &cmp::Operator, value: &Value<'_>) -> bool {
        match &self {
            Column::String(meta, data) => {
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
            Column::Float(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_f64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, v))
            }
            Column::Integer(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_i64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, v))
            }
            Column::Unsigned(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_u64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, v))
            }
            Column::Bool(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                match value {
                    Value::Null => false,
                    Value::Boolean(b) => meta.might_match_all_values(op, *b),
                    v => panic!("cannot compare on boolean column using {:?}", v),
                }
            }
            Column::ByteArray(meta, _) => todo!(),
        }
    }

    // Helper method to determine if the predicate can not possibly match any
    // values in the column.
    fn predicate_matches_no_values(&self, op: &cmp::Operator, value: &Value<'_>) -> bool {
        match &self {
            Column::String(meta, data) => {
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
            Column::Float(meta, data) => meta.match_no_values(op, value.scalar().as_f64()),
            Column::Integer(meta, data) => meta.match_no_values(op, value.scalar().as_i64()),
            Column::Unsigned(meta, data) => meta.match_no_values(op, value.scalar().as_u64()),
            Column::Bool(meta, data) => meta.match_no_values(op, value.bool()),
            Column::ByteArray(meta, _) => todo!(),
        }
    }

    //
    // Methods for selecting
    //

    /// The minimum value present within the set of rows.
    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Column::String(_, data) => data.min(row_ids),
            Column::Float(_, data) => data.min(row_ids),
            Column::Integer(_, data) => data.min(row_ids),
            Column::Unsigned(_, data) => data.min(row_ids),
            Column::Bool(_, data) => data.min(row_ids),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    /// The minimum value present within the set of rows.
    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Column::String(_, data) => data.max(row_ids),
            Column::Float(_, data) => data.max(row_ids),
            Column::Integer(_, data) => data.max(row_ids),
            Column::Unsigned(_, data) => data.max(row_ids),
            Column::Bool(_, data) => data.max(row_ids),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    //
    // Methods for aggregating
    //

    /// The summation of all non-null values located at the provided rows.
    pub fn sum(&self, row_ids: &[u32]) -> Scalar {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Column::Float(_, data) => data.sum(row_ids),
            Column::Integer(_, data) => data.sum(row_ids),
            Column::Unsigned(_, data) => data.sum(row_ids),
            _ => panic!("cannot sum non-numerical column type"),
        }
    }

    /// The count of all non-null values located at the provided rows.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        assert!(row_ids.len() as u32 <= self.num_rows());

        match &self {
            Column::String(_, data) => data.count(row_ids),
            Column::Float(_, data) => data.count(row_ids),
            Column::Integer(_, data) => data.count(row_ids),
            Column::Unsigned(_, data) => data.count(row_ids),
            Column::Bool(_, data) => data.count(row_ids),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    //
    // Methods for inspecting
    //

    /// Determines if this column contains any NULL values.
    pub fn contains_null(&self) -> bool {
        match &self {
            Column::String(_, data) => data.contains_null(),
            Column::Float(_, data) => data.contains_null(),
            Column::Integer(_, data) => data.contains_null(),
            Column::Unsigned(_, data) => data.contains_null(),
            Column::Bool(_, data) => data.contains_null(),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    /// Determines if the column has a non-null value at any of the provided
    /// row ids.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match &self {
            Column::String(_, data) => data.has_non_null_value(row_ids),
            Column::Float(_, data) => data.has_non_null_value(row_ids),
            Column::Integer(_, data) => data.has_non_null_value(row_ids),
            Column::Unsigned(_, data) => data.has_non_null_value(row_ids),
            Column::Bool(_, data) => data.has_non_null_value(row_ids),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    /// Determines if the column has a non-null value on any row.
    pub fn has_any_non_null_value(&self) -> bool {
        match &self {
            Column::String(_, data) => data.has_any_non_null_value(),
            Column::Float(_, data) => data.has_any_non_null_value(),
            Column::Integer(_, data) => data.has_any_non_null_value(),
            Column::Unsigned(_, data) => data.has_any_non_null_value(),
            Column::Bool(_, data) => data.has_any_non_null_value(),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    /// Determines if the column contains other values than those provided in
    /// `values`.
    pub fn contains_other_values(&self, values: &BTreeSet<Option<&String>>) -> bool {
        todo!()
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
    // The total size of the column in bytes.
    size: u64,

    // The total number of rows in the column.
    rows: u32,

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
pub enum StringEncoding {
    RLEDictionary(dictionary::RLE),
    Dictionary(dictionary::Plain),
    // TODO - simple array encoding, e.g., via Arrow String array.
}

/// This implementation is concerned with how to produce string columns with
/// different encodings.
impl StringEncoding {
    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        match self {
            StringEncoding::RLEDictionary(enc) => enc.contains_null(),
            StringEncoding::Dictionary(enc) => enc.contains_null(),
        }
    }

    /// Determines if the column contains a non-null value
    pub fn has_any_non_null_value(&self) -> bool {
        match &self {
            Self::RLEDictionary(c) => c.has_any_non_null_value(),
            Self::Dictionary(c) => c.has_any_non_null_value(),
        }
    }

    /// Determines if the column contains a non-null value at one of the
    /// provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        match &self {
            Self::RLEDictionary(c) => c.has_non_null_value(row_ids),
            Self::Dictionary(c) => c.has_non_null_value(row_ids),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.value(row_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.value(row_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
        }
    }

    /// All values present at the provided logical row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values<'_> {
        match &self {
            Self::RLEDictionary(c) => Values::String(c.values(row_ids, vec![])),
            Self::Dictionary(c) => Values::String(c.values(row_ids, vec![])),
        }
    }

    /// All values in the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_values(&self) -> Values<'_> {
        match &self {
            Self::RLEDictionary(c) => Values::String(c.all_values(vec![])),
            Self::Dictionary(c) => Values::String(c.all_values(vec![])),
        }
    }

    /// Returns the logical value for the specified encoded representation.
    pub fn decode_id(&self, encoded_id: u32) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.decode_id(encoded_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.decode_id(encoded_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
        }
    }

    /// Returns the distinct set of values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination sets.
    pub fn distinct_values(&self, row_ids: &[u32]) -> ValueSet<'_> {
        match &self {
            Self::RLEDictionary(c) => ValueSet::String(c.distinct_values(row_ids, BTreeSet::new())),
            Self::Dictionary(c) => ValueSet::String(c.distinct_values(row_ids, BTreeSet::new())),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    pub fn row_ids_filter(&self, op: &cmp::Operator, value: &str, dst: RowIDs) -> RowIDs {
        match &self {
            Self::RLEDictionary(c) => c.row_ids_filter(value, op, dst),
            Self::Dictionary(c) => c.row_ids_filter(value, op, dst),
        }
    }

    /// The lexicographic minimum non-null value at the rows specified, or the
    /// NULL value if the column only contains NULL values at the provided row
    /// ids.
    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.min(row_ids) {
                Some(min) => Value::String(min),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.min(row_ids) {
                Some(min) => Value::String(min),
                None => Value::Null,
            },
        }
    }

    /// The lexicographic maximum non-null value at the rows specified, or the
    /// NULL value if the column only contains NULL values at the provided row
    /// ids.
    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            Self::RLEDictionary(c) => match c.max(row_ids) {
                Some(max) => Value::String(max),
                None => Value::Null,
            },
            Self::Dictionary(c) => match c.max(row_ids) {
                Some(max) => Value::String(max),
                None => Value::Null,
            },
        }
    }

    /// The number of non-null values at the provided row ids.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            Self::RLEDictionary(c) => c.count(row_ids),
            Self::Dictionary(c) => c.count(row_ids),
        }
    }

    /// Calculate all row ids for each distinct value in the column.
    pub fn group_row_ids(&self) -> Either<Vec<&RowIDs>, Vec<RowIDs>> {
        match self {
            Self::RLEDictionary(enc) => Either::Left(enc.group_row_ids()),
            Self::Dictionary(enc) => Either::Right(enc.group_row_ids()),
        }
    }

    fn from_arrow_string_array(arr: &arrow::array::StringArray) -> Self {
        // build a sorted dictionary.
        let mut dictionary = BTreeSet::new();

        for i in 0..arr.len() {
            if !arr.is_null(i) {
                dictionary.insert(arr.value(i).to_string());
            }
        }

        let mut data: dictionary::Encoding =
            if dictionary.len() > TEMP_CARDINALITY_DICTIONARY_ENCODING_LIMIT {
                dictionary::Encoding::Plain(dictionary::Plain::with_dictionary(dictionary))
            } else {
                dictionary::Encoding::RLE(dictionary::RLE::with_dictionary(dictionary))
            };

        let mut prev = if !arr.is_null(0) {
            Some(arr.value(0))
        } else {
            None
        };

        let mut count = 1;
        for i in 1..arr.len() {
            let next = if arr.is_null(i) {
                None
            } else {
                Some(arr.value(i))
            };

            if prev == next {
                count += 1;
                continue;
            }

            match prev {
                Some(x) => data.push_additional(Some(x.to_string()), count),
                None => data.push_additional(None, count),
            }
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        match prev {
            Some(x) => data.push_additional(Some(x.to_string()), count),
            None => data.push_additional(None, count),
        };

        // TODO(edd): size of RLE column.
        let dictionary = data.dictionary();
        let range = if !dictionary.is_empty() {
            let min = data.dictionary()[0].clone();
            let max = data.dictionary()[data.dictionary().len() - 1].clone();
            Some((min, max))
        } else {
            None
        };

        let meta = MetaData {
            rows: data.num_rows(),
            range,
            ..MetaData::default()
        };

        // TODO(edd): consider just storing under the `StringEncoding` a
        // `Dictionary` variant that would be a `dictionary::Encoding`.
        match data {
            dictionary::Encoding::RLE(enc) => Self::RLEDictionary(enc),
            dictionary::Encoding::Plain(enc) => Self::Dictionary(enc),
        }
    }

    /// All encoded values for the provided logical row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn encoded_values(&self, row_ids: &[u32], dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RLEDictionary(c) => c.encoded_values(row_ids, dst),
            Self::Dictionary(c) => c.encoded_values(row_ids, dst),
        }
    }

    /// All encoded values for the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_encoded_values(&self, dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RLEDictionary(c) => c.all_encoded_values(dst),
            Self::Dictionary(c) => c.all_encoded_values(dst),
        }
    }

    fn from_opt_strs(arr: &[Option<&str>]) -> Self {
        //
        // TODO(edd): potentially switch on things like cardinality in the input
        // and encode in different ways. Right now we only encode with RLE.
        //

        // RLE creation.

        // build a sorted dictionary.
        let mut dictionary = BTreeSet::new();

        for v in arr {
            if let Some(x) = v {
                dictionary.insert(x.to_string());
            }
        }

        let mut data = dictionary::RLE::with_dictionary(dictionary);

        let mut prev = &arr[0];

        let mut count = 1;
        for next in arr[1..].iter() {
            if prev == next {
                count += 1;
                continue;
            }

            match prev {
                Some(x) => data.push_additional(Some(x.to_string()), count),
                None => data.push_additional(None, count),
            }
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        match prev {
            Some(x) => data.push_additional(Some(x.to_string()), count),
            None => data.push_additional(None, count),
        };

        Self::RLEDictionary(data)
    }

    fn from_strs(arr: &[&str]) -> Self {
        //
        // TODO(edd): potentially switch on things like cardinality in the input
        // and encode in different ways. Right now we only encode with RLE.
        //

        // RLE creation.

        // build a sorted dictionary.
        let dictionary = arr.iter().map(|x| x.to_string()).collect::<BTreeSet<_>>();
        let mut data = dictionary::RLE::with_dictionary(dictionary);

        let mut prev = &arr[0];
        let mut count = 1;
        for next in arr[1..].iter() {
            if prev == next {
                count += 1;
                continue;
            }

            data.push_additional(Some(prev.to_string()), count);
            prev = next;
            count = 1;
        }

        // Add final batch to column if any
        data.push_additional(Some(prev.to_string()), count);

        Self::RLEDictionary(data)
    }

    // generates metadata for an encoded column.
    pub fn meta_from_data(data: &Self) -> MetaData<String> {
        match data {
            Self::RLEDictionary(data) => {
                let dictionary = data.dictionary();
                let range = if !dictionary.is_empty() {
                    let min = data.dictionary()[0].clone();
                    let max = data.dictionary()[data.dictionary().len() - 1].clone();
                    Some((min, max))
                } else {
                    None
                };

                MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range,
                    properties: ColumnProperties {
                        has_pre_computed_row_ids: true,
                    },
                }
            }
            Self::Dictionary(data) => {
                let dictionary = data.dictionary();
                let range = if !dictionary.is_empty() {
                    let min = data.dictionary()[0].clone();
                    let max = data.dictionary()[data.dictionary().len() - 1].clone();
                    Some((min, max))
                } else {
                    None
                };

                MetaData {
                    rows: data.num_rows(),
                    range,
                    ..MetaData::default()
                }
            }
        }
    }
}

impl std::fmt::Display for StringEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RLEDictionary(data) => write!(f, "{}", data),
            Self::Dictionary(data) => write!(f, "{}", data),
        }
    }
}

// Converts an Arrow `StringArray` into a column, currently using the RLE
// encoding scheme. Other encodings can be supported and added to this
// implementation.
//
// Note: this currently runs through the array and builds the dictionary before
// creating the encoding. There is room for performance improvement here but
// ideally it's a "write once read many" scenario.
impl From<arrow::array::StringArray> for Column {
    fn from(arr: arrow::array::StringArray) -> Self {
        let data = StringEncoding::from_arrow_string_array(&arr);
        Column::String(StringEncoding::meta_from_data(&data), data)
    }
}

impl From<&[Option<&str>]> for Column {
    fn from(arr: &[Option<&str>]) -> Self {
        let data = StringEncoding::from_opt_strs(arr);
        Column::String(StringEncoding::meta_from_data(&data), data)
    }
}

impl From<&[Option<String>]> for Column {
    fn from(arr: &[Option<String>]) -> Self {
        let other = arr.iter().map(|x| x.as_deref()).collect::<Vec<_>>();
        Self::from(other.as_slice())
    }
}

impl From<&[&str]> for Column {
    fn from(arr: &[&str]) -> Self {
        let data = StringEncoding::from_strs(arr);
        Column::String(StringEncoding::meta_from_data(&data), data)
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
            size: data.size(),
            rows: data.num_rows(),
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
            size: data.size(),
            rows: data.num_rows(),
            range,
            properties: ColumnProperties::default(),
        };

        Column::Unsigned(meta, data)
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
            size: data.size(),
            rows: data.num_rows(),
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
            size: data.size(),
            rows: data.num_rows(),
            range,
            properties: ColumnProperties::default(),
        };

        Column::Integer(meta, data)
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
            size: data.size(),
            rows: data.num_rows(),
            range: Some((min, max)),
            properties: ColumnProperties::default(),
        };

        Column::Float(meta, data)
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

        let data = fixed_null::FixedNull::<arrow::datatypes::Float64Type>::from(arr);
        let meta = MetaData {
            size: data.size(),
            rows: data.num_rows(),
            range,
            ..MetaData::default()
        };

        Column::Float(meta, FloatEncoding::FixedNull64(data))
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
            size: data.size(),
            rows: data.num_rows(),
            range,
            ..MetaData::default()
        };
        Column::Bool(meta, data)
    }
}

/// These variants hold aggregates, which are the results of applying aggregates
/// to column data.
#[derive(Debug, Copy, Clone, PartialEq)]
pub enum AggregateResult<'a> {
    // Any type of column can have rows counted. NULL values do not contribute
    // to the count. If all rows are NULL then count will be `0`.
    Count(u64),

    // Only numerical columns with scalar values can be summed. NULL values do
    // not contribute to the sum, but if all rows are NULL then the sum is
    // itself NULL (represented by `None`).
    Sum(Scalar),

    // The minimum value in the column data.
    Min(Value<'a>),

    // The maximum value in the column data.
    Max(Value<'a>),

    // The first value in the column data and the corresponding timestamp.
    First(Option<(i64, Value<'a>)>),

    // The last value in the column data and the corresponding timestamp.
    Last(Option<(i64, Value<'a>)>),
}

#[allow(unused_assignments)]
impl<'a> AggregateResult<'a> {
    pub fn update(&mut self, other: Value<'a>) {
        if other.is_null() {
            // a NULL value has no effect on aggregates
            return;
        }

        match self {
            Self::Count(v) => {
                if !other.is_null() {
                    *v += 1;
                }
            }
            Self::Min(v) => match (&v, &other) {
                (Value::Null, _) => {
                    // something is always smaller than NULL
                    *v = other;
                }
                (Value::String(_), Value::Null) => {} // do nothing
                (Value::String(a), Value::String(b)) => {
                    if a.cmp(b) == std::cmp::Ordering::Greater {
                        *v = other;
                    }
                }
                (Value::String(a), Value::ByteArray(b)) => {
                    if a.as_bytes().cmp(b) == std::cmp::Ordering::Greater {
                        *v = other;
                    }
                }
                (Value::ByteArray(_), Value::Null) => {} // do nothing
                (Value::ByteArray(a), Value::String(b)) => {
                    if a.cmp(&b.as_bytes()) == std::cmp::Ordering::Greater {
                        *v = other;
                    }
                }
                (Value::ByteArray(a), Value::ByteArray(b)) => {
                    if a.cmp(b) == std::cmp::Ordering::Greater {
                        *v = other;
                    }
                }
                (Value::Scalar(_), Value::Null) => {} // do nothing
                (Value::Scalar(a), Value::Scalar(b)) => {
                    if a > b {
                        *v = other;
                    }
                }
                (_, _) => unreachable!("not a possible variant combination"),
            },
            Self::Max(v) => match (&v, &other) {
                (Value::Null, _) => {
                    // something is always larger than NULL
                    *v = other;
                }
                (Value::String(_), Value::Null) => {} // do nothing
                (Value::String(a), Value::String(b)) => {
                    if a.cmp(b) == std::cmp::Ordering::Less {
                        *v = other;
                    }
                }
                (Value::String(a), Value::ByteArray(b)) => {
                    if a.as_bytes().cmp(b) == std::cmp::Ordering::Less {
                        *v = other;
                    }
                }
                (Value::ByteArray(_), Value::Null) => {} // do nothing
                (Value::ByteArray(a), Value::String(b)) => {
                    if a.cmp(&b.as_bytes()) == std::cmp::Ordering::Less {
                        *v = other;
                    }
                }
                (Value::ByteArray(a), Value::ByteArray(b)) => {
                    if a.cmp(b) == std::cmp::Ordering::Less {
                        *v = other;
                    }
                }
                (Value::Scalar(_), Value::Null) => {} // do nothing
                (Value::Scalar(a), Value::Scalar(b)) => {
                    if a < b {
                        *v = other;
                    }
                }
                (_, _) => unreachable!("not a possible variant combination"),
            },
            Self::Sum(v) => match (&v, &other) {
                (Scalar::Null, Value::Scalar(other_scalar)) => {
                    // NULL + something  == something
                    *v = *other_scalar;
                }
                (_, Value::Scalar(b)) => *v += b,
                (_, _) => unreachable!("not a possible variant combination"),
            },
            _ => unimplemented!("First and Last aggregates not implemented yet"),
        }
    }

    /// Merge `other` into `self`
    pub fn merge(&mut self, other: &AggregateResult<'a>) {
        match (self, other) {
            (AggregateResult::Count(this), AggregateResult::Count(that)) => *this += *that,
            (AggregateResult::Sum(this), AggregateResult::Sum(that)) => *this += that,
            (AggregateResult::Min(this), AggregateResult::Min(that)) => {
                if *this > *that {
                    *this = *that;
                }
            }
            (AggregateResult::Max(this), AggregateResult::Max(that)) => {
                if *this < *that {
                    *this = *that;
                }
            }
            (a, b) => unimplemented!("merging {:?} into {:?} not yet implemented", b, a),
        }
    }

    pub fn try_as_str(&self) -> Option<&str> {
        match &self {
            AggregateResult::Min(v) => match v {
                Value::Null => None,
                Value::String(s) => Some(s),
                v => panic!("cannot convert {:?} to &str", v),
            },
            AggregateResult::Max(v) => match v {
                Value::Null => None,
                Value::String(s) => Some(s),
                v => panic!("cannot convert {:?} to &str", v),
            },
            AggregateResult::First(_) => panic!("cannot convert first tuple to &str"),
            AggregateResult::Last(_) => panic!("cannot convert last tuple to &str"),
            AggregateResult::Sum(v) => panic!("cannot convert {:?} to &str", v),
            AggregateResult::Count(_) => panic!("cannot convert count to &str"),
        }
    }

    pub fn try_as_bytes(&self) -> Option<&[u8]> {
        match &self {
            AggregateResult::Min(v) => match v {
                Value::Null => None,
                Value::ByteArray(s) => Some(s),
                v => panic!("cannot convert {:?} to &[u8]", v),
            },
            AggregateResult::Max(v) => match v {
                Value::Null => None,
                Value::ByteArray(s) => Some(s),
                v => panic!("cannot convert {:?} to &[u8]", v),
            },
            AggregateResult::First(_) => panic!("cannot convert first tuple to &[u8]"),
            AggregateResult::Last(_) => panic!("cannot convert last tuple to &[u8]"),
            AggregateResult::Sum(v) => panic!("cannot convert {:?} to &[u8]", v),
            AggregateResult::Count(_) => panic!("cannot convert count to &[u8]"),
        }
    }

    pub fn try_as_bool(&self) -> Option<bool> {
        match &self {
            AggregateResult::Min(v) => match v {
                Value::Null => None,
                Value::Boolean(s) => Some(*s),
                v => panic!("cannot convert {:?} to bool", v),
            },
            AggregateResult::Max(v) => match v {
                Value::Null => None,
                Value::Boolean(s) => Some(*s),
                v => panic!("cannot convert {:?} to bool", v),
            },
            AggregateResult::First(_) => panic!("cannot convert first tuple to bool"),
            AggregateResult::Last(_) => panic!("cannot convert last tuple to bool"),
            AggregateResult::Sum(v) => panic!("cannot convert {:?} to bool", v),
            AggregateResult::Count(_) => panic!("cannot convert count to bool"),
        }
    }

    pub fn try_as_i64_scalar(&self) -> Option<i64> {
        match &self {
            AggregateResult::Sum(v) => match v {
                Scalar::Null => None,
                Scalar::I64(v) => Some(*v),
                v => panic!("cannot convert {:?} to i64", v),
            },
            AggregateResult::Min(v) => match v {
                Value::Null => None,
                Value::Scalar(s) => match s {
                    Scalar::Null => None,
                    Scalar::I64(v) => Some(*v),
                    v => panic!("cannot convert {:?} to u64", v),
                },
                v => panic!("cannot convert {:?} to i64", v),
            },
            AggregateResult::Max(v) => match v {
                Value::Null => None,
                Value::Scalar(s) => match s {
                    Scalar::Null => None,
                    Scalar::I64(v) => Some(*v),
                    v => panic!("cannot convert {:?} to u64", v),
                },
                v => panic!("cannot convert {:?} to i64", v),
            },
            AggregateResult::First(_) => panic!("cannot convert first tuple to scalar"),
            AggregateResult::Last(_) => panic!("cannot convert last tuple to scalar"),
            AggregateResult::Count(_) => panic!("cannot represent count as i64"),
        }
    }

    pub fn try_as_u64_scalar(&self) -> Option<u64> {
        match &self {
            AggregateResult::Sum(v) => match v {
                Scalar::Null => None,
                Scalar::U64(v) => Some(*v),
                v => panic!("cannot convert {:?} to u64", v),
            },
            AggregateResult::Count(c) => Some(*c),
            AggregateResult::Min(v) => match v {
                Value::Null => None,
                Value::Scalar(s) => match s {
                    Scalar::Null => None,
                    Scalar::U64(v) => Some(*v),
                    v => panic!("cannot convert {:?} to u64", v),
                },
                v => panic!("cannot convert {:?} to u64", v),
            },
            AggregateResult::Max(v) => match v {
                Value::Null => None,
                Value::Scalar(s) => match s {
                    Scalar::Null => None,
                    Scalar::U64(v) => Some(*v),
                    v => panic!("cannot convert {:?} to u64", v),
                },
                v => panic!("cannot convert {:?} to u64", v),
            },
            AggregateResult::First(_) => panic!("cannot convert first tuple to scalar"),
            AggregateResult::Last(_) => panic!("cannot convert last tuple to scalar"),
        }
    }

    pub fn try_as_f64_scalar(&self) -> Option<f64> {
        match &self {
            AggregateResult::Sum(v) => match v {
                Scalar::Null => None,
                Scalar::F64(v) => Some(*v),
                v => panic!("cannot convert {:?} to f64", v),
            },
            AggregateResult::Min(v) => match v {
                Value::Null => None,
                Value::Scalar(s) => match s {
                    Scalar::Null => None,
                    Scalar::F64(v) => Some(*v),
                    v => panic!("cannot convert {:?} to f64", v),
                },
                v => panic!("cannot convert {:?} to f64", v),
            },
            AggregateResult::Max(v) => match v {
                Value::Null => None,
                Value::Scalar(s) => match s {
                    Scalar::Null => None,
                    Scalar::F64(v) => Some(*v),
                    v => panic!("cannot convert {:?} to f64", v),
                },
                v => panic!("cannot convert {:?} to f64", v),
            },
            AggregateResult::First(_) => panic!("cannot convert first tuple to scalar"),
            AggregateResult::Last(_) => panic!("cannot convert last tuple to scalar"),
            AggregateResult::Count(_) => panic!("cannot represent count as f64"),
        }
    }
}

impl From<&AggregateType> for AggregateResult<'_> {
    fn from(typ: &AggregateType) -> Self {
        match typ {
            AggregateType::Count => Self::Count(0),
            AggregateType::First => Self::First(None),
            AggregateType::Last => Self::Last(None),
            AggregateType::Min => Self::Min(Value::Null),
            AggregateType::Max => Self::Max(Value::Null),
            AggregateType::Sum => Self::Sum(Scalar::Null),
        }
    }
}

impl std::fmt::Display for AggregateResult<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateResult::Count(v) => write!(f, "{}", v),
            AggregateResult::First(v) => match v {
                Some((_, v)) => write!(f, "{}", v),
                None => write!(f, "NULL"),
            },
            AggregateResult::Last(v) => match v {
                Some((_, v)) => write!(f, "{}", v),
                None => write!(f, "NULL"),
            },
            AggregateResult::Min(v) => write!(f, "{}", v),
            AggregateResult::Max(v) => write!(f, "{}", v),
            AggregateResult::Sum(v) => write!(f, "{}", v),
        }
    }
}

/// A scalar is a numerical value that can be aggregated.
#[derive(Debug, PartialEq, PartialOrd, Copy, Clone)]
pub enum Scalar {
    Null,
    I64(i64),
    U64(u64),
    F64(f64),
}

macro_rules! typed_scalar_converters {
    ($(($name:ident, $try_name:ident, $type:ident),)*) => {
        $(
            fn $name(&self) -> $type {
                match &self {
                    Self::I64(v) => $type::try_from(*v).unwrap(),
                    Self::U64(v) => $type::try_from(*v).unwrap(),
                    Self::F64(v) => panic!("cannot convert Self::F64"),
                    Self::Null => panic!("cannot convert Scalar::Null"),
                }
            }

            fn $try_name(&self) -> Option<$type> {
                match &self {
                    Self::I64(v) => $type::try_from(*v).ok(),
                    Self::U64(v) => $type::try_from(*v).ok(),
                    Self::F64(v) => panic!("cannot convert Self::F64"),
                    Self::Null => None,
                }
            }
        )*
    };
}

impl Scalar {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    // Implementations of all the accessors for the variants of `Scalar`.
    typed_scalar_converters! {
        (as_i64, try_as_i64, i64),
        (as_i32, try_as_i32, i32),
        (as_i16, try_as_i16, i16),
        (as_i8, try_as_i8, i8),
        (as_u64, try_as_u64, u64),
        (as_u32, try_as_u32, u32),
        (as_u16, try_as_u16, u16),
        (as_u8, try_as_u8, u8),
    }

    fn as_f64(&self) -> f64 {
        match &self {
            Scalar::F64(v) => *v,
            _ => unimplemented!("converting integer Scalar to f64 unsupported"),
        }
    }

    fn try_as_f64(&self) -> Option<f64> {
        match &self {
            Scalar::F64(v) => Some(*v),
            _ => unimplemented!("converting integer Scalar to f64 unsupported"),
        }
    }
}

impl std::ops::AddAssign<&Scalar> for Scalar {
    fn add_assign(&mut self, rhs: &Scalar) {
        if rhs.is_null() {
            // Adding NULL does nothing.
            return;
        }

        match self {
            Scalar::F64(v) => {
                if let Scalar::F64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            Scalar::I64(v) => {
                if let Scalar::I64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            Scalar::U64(v) => {
                if let Scalar::U64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            _ => unimplemented!("unsupported and to be removed"),
        }
    }
}

impl<'a> std::ops::AddAssign<&Scalar> for &mut Scalar {
    fn add_assign(&mut self, rhs: &Scalar) {
        match self {
            Scalar::F64(v) => {
                if let Scalar::F64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            Scalar::I64(v) => {
                if let Scalar::I64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            Scalar::U64(v) => {
                if let Scalar::U64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            _ => unimplemented!("unsupported and to be removed"),
        }
    }
}

impl std::fmt::Display for Scalar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Scalar::Null => write!(f, "NULL"),
            Scalar::I64(v) => write!(f, "{}", v),
            Scalar::U64(v) => write!(f, "{}", v),
            Scalar::F64(v) => write!(f, "{}", v),
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum OwnedValue {
    // Represents a NULL value in a column row.
    Null,

    // A UTF-8 valid string.
    String(String),

    // An arbitrary byte array.
    ByteArray(Vec<u8>),

    // A boolean value.
    Boolean(bool),

    // A numeric scalar value.
    Scalar(Scalar),
}

impl PartialEq<Value<'_>> for OwnedValue {
    fn eq(&self, other: &Value<'_>) -> bool {
        match (&self, other) {
            (OwnedValue::String(a), Value::String(b)) => a == b,
            (OwnedValue::Scalar(a), Value::Scalar(b)) => a == b,
            (OwnedValue::Boolean(a), Value::Boolean(b)) => a == b,
            (OwnedValue::ByteArray(a), Value::ByteArray(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd<Value<'_>> for OwnedValue {
    fn partial_cmp(&self, other: &Value<'_>) -> Option<std::cmp::Ordering> {
        match (&self, other) {
            (OwnedValue::String(a), Value::String(b)) => Some(a.as_str().cmp(b)),
            (OwnedValue::Scalar(a), Value::Scalar(b)) => a.partial_cmp(b),
            (OwnedValue::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (OwnedValue::ByteArray(a), Value::ByteArray(b)) => a.as_slice().partial_cmp(*b),
            _ => None,
        }
    }
}

/// Each variant is a possible value type that can be returned from a column.
#[derive(Debug, PartialEq, PartialOrd, Copy, Clone)]
pub enum Value<'a> {
    // Represents a NULL value in a column row.
    Null,

    // A UTF-8 valid string.
    String(&'a str),

    // An arbitrary byte array.
    ByteArray(&'a [u8]),

    // A boolean value.
    Boolean(bool),

    // A numeric scalar value.
    Scalar(Scalar),
}

impl Value<'_> {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn scalar(&self) -> &Scalar {
        if let Self::Scalar(s) = self {
            return s;
        }
        panic!("cannot unwrap Value to Scalar");
    }

    pub fn string(&self) -> &str {
        if let Self::String(s) = self {
            return s;
        }
        panic!("cannot unwrap Value to String");
    }

    pub fn bool(&self) -> bool {
        if let Self::Boolean(b) = self {
            return *b;
        }
        panic!("cannot unwrap Value to Scalar");
    }
}

impl std::fmt::Display for Value<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::String(s) => write!(f, "{}", s),
            Value::ByteArray(arr) => write!(f, "{:?}", arr),
            Value::Boolean(b) => write!(f, "{}", b),
            Value::Scalar(s) => match s {
                Scalar::I64(v) => write!(f, "{}", v),
                Scalar::U64(v) => write!(f, "{}", v),
                Scalar::F64(v) => write!(f, "{}", v),
                Scalar::Null => write!(f, "NULL"),
            },
        }
    }
}

impl<'a> From<&'a str> for Value<'a> {
    fn from(v: &'a str) -> Self {
        Self::String(v)
    }
}

// Implementations of From trait for various concrete types.
macro_rules! scalar_from_impls {
    ($(($variant:ident, $type:ident),)*) => {
        $(
            impl From<$type> for Value<'_> {
                fn from(v: $type) -> Self {
                    Self::Scalar(Scalar::$variant(v))
                }
            }

            impl From<Option<$type>> for Value<'_> {
                fn from(v: Option<$type>) -> Self {
                    match v {
                        Some(v) => Self::Scalar(Scalar::$variant(v)),
                        None => Self::Null,
                    }
                }
            }
        )*
    };
}

scalar_from_impls! {
    (I64, i64),
    (U64, u64),
    (F64, f64),
}

/// Each variant is a typed vector of materialised values for a column.
#[derive(Debug, PartialEq)]
pub enum Values<'a> {
    // UTF-8 valid unicode strings
    String(Vec<Option<&'a str>>),

    // Scalar types
    I64(Vec<i64>),
    U64(Vec<u64>),
    F64(Vec<f64>),
    I64N(Vec<Option<i64>>),
    U64N(Vec<Option<u64>>),
    F64N(Vec<Option<f64>>),

    // Boolean values
    Bool(Vec<Option<bool>>),

    // Arbitrary byte arrays
    ByteArray(Vec<Option<&'a [u8]>>),
}

impl<'a> Values<'a> {
    pub fn len(&self) -> usize {
        match &self {
            Self::String(c) => c.len(),
            Self::I64(c) => c.len(),
            Self::U64(c) => c.len(),
            Self::F64(c) => c.len(),
            Self::Bool(c) => c.len(),
            Self::ByteArray(c) => c.len(),
            Self::I64N(c) => c.len(),
            Self::U64N(c) => c.len(),
            Self::F64N(c) => c.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn value(&self, i: usize) -> Value<'a> {
        match &self {
            Self::String(c) => match c[i] {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
            Self::F64(c) => Value::Scalar(Scalar::F64(c[i])),
            Self::I64(c) => Value::Scalar(Scalar::I64(c[i])),
            Self::U64(c) => Value::Scalar(Scalar::U64(c[i])),
            Self::Bool(c) => match c[i] {
                Some(v) => Value::Boolean(v),
                None => Value::Null,
            },
            Self::ByteArray(c) => match c[i] {
                Some(v) => Value::ByteArray(v),
                None => Value::Null,
            },
            Self::I64N(c) => match c[i] {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
            Self::U64N(c) => match c[i] {
                Some(v) => Value::Scalar(Scalar::U64(v)),
                None => Value::Null,
            },
            Self::F64N(c) => match c[i] {
                Some(v) => Value::Scalar(Scalar::F64(v)),
                None => Value::Null,
            },
        }
    }
}

/// Moves ownership of Values into an arrow `ArrayRef`.
impl From<Values<'_>> for array::ArrayRef {
    fn from(values: Values<'_>) -> Self {
        match values {
            Values::String(values) => Arc::new(arrow::array::StringArray::from(values)),
            Values::I64(values) => Arc::new(arrow::array::Int64Array::from(values)),
            Values::U64(values) => Arc::new(arrow::array::UInt64Array::from(values)),
            Values::F64(values) => Arc::new(arrow::array::Float64Array::from(values)),
            Values::I64N(values) => Arc::new(arrow::array::Int64Array::from(values)),
            Values::U64N(values) => Arc::new(arrow::array::UInt64Array::from(values)),
            Values::F64N(values) => Arc::new(arrow::array::Float64Array::from(values)),
            Values::Bool(values) => Arc::new(arrow::array::BooleanArray::from(values)),
            Values::ByteArray(values) => Arc::new(arrow::array::BinaryArray::from(values)),
        }
    }
}

pub struct ValuesIterator<'a> {
    v: &'a Values<'a>,
    next_i: usize,
}

impl<'a> ValuesIterator<'a> {
    pub fn new(v: &'a Values<'a>) -> Self {
        Self { v, next_i: 0 }
    }
}
impl<'a> Iterator for ValuesIterator<'a> {
    type Item = Value<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let curr_i = self.next_i;
        self.next_i += 1;

        if curr_i == self.v.len() {
            return None;
        }

        Some(self.v.value(curr_i))
    }
}

#[derive(PartialEq, Debug)]
pub enum ValueSet<'a> {
    // UTF-8 valid unicode strings
    String(BTreeSet<Option<&'a String>>),

    // Arbitrary collections of bytes
    ByteArray(BTreeSet<Option<&'a [u8]>>),
}

#[derive(Debug, PartialEq)]
/// A representation of encoded values for a column.
pub enum EncodedValues {
    I64(Vec<i64>),
    U32(Vec<u32>),
}

impl EncodedValues {
    pub fn with_capacity_i64(capacity: usize) -> Self {
        Self::I64(Vec::with_capacity(capacity))
    }

    pub fn with_capacity_u32(capacity: usize) -> Self {
        Self::U32(Vec::with_capacity(capacity))
    }

    pub fn as_i64(&self) -> &Vec<i64> {
        if let Self::I64(arr) = self {
            return arr;
        }
        panic!("cannot borrow &Vec<i64>");
    }

    pub fn as_u32(&self) -> &Vec<u32> {
        if let Self::U32(arr) = self {
            return arr;
        }
        panic!("cannot borrow &Vec<u32>");
    }

    /// Takes a `Vec<u32>` out of the enum.
    pub fn take_u32(&mut self) -> Vec<u32> {
        std::mem::take(match self {
            Self::I64(a) => panic!("cannot take Vec<u32> out of I64 variant"),
            Self::U32(arr) => arr,
        })
    }

    pub fn len(&self) -> usize {
        match self {
            Self::I64(v) => v.len(),
            Self::U32(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::I64(v) => v.is_empty(),
            Self::U32(v) => v.is_empty(),
        }
    }

    pub fn clear(&mut self) {
        match self {
            Self::I64(v) => v.clear(),
            Self::U32(v) => v.clear(),
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        match self {
            Self::I64(v) => v.reserve(additional),
            Self::U32(v) => v.reserve(additional),
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
        if let Self::Some(ids) = self {
            return ids;
        }
        panic!("cannot unwrap RowIDsOption to RowIDs");
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
            Self::Bitmap(bm) => panic!("not supported yet"),
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

    pub fn intersect(&mut self, other: &RowIDs) {
        match (self, other) {
            (Self::Bitmap(_self), RowIDs::Bitmap(ref other)) => _self.and_inplace(other),
            (_, _) => unimplemented!("currently unsupported"),
        };
    }

    pub fn union(&mut self, other: &RowIDs) {
        match (self, other) {
            (Self::Bitmap(inner), RowIDs::Bitmap(other)) => inner.or_inplace(other),
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

#[cfg(test)]
mod test {
    use super::*;
    use arrow_deps::arrow::array::{Int64Array, StringArray};

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
    fn column_properties() {
        let data = StringEncoding::RLEDictionary(dictionary::RLE::default());
        let meta = StringEncoding::meta_from_data(&data);
        let col = Column::String(meta, data);
        assert!(col.properties().has_pre_computed_row_ids);
    }

    #[test]
    fn from_arrow_string_array() {
        let input = vec![None, Some("world"), None, Some("hello")];
        let arr = StringArray::from(input);

        let col = Column::from(arr);
        if let Column::String(meta, StringEncoding::RLEDictionary(enc)) = col {
            assert_eq!(
                meta,
                super::MetaData::<String> {
                    size: 317,
                    rows: 4,
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
        if let Column::String(meta, StringEncoding::RLEDictionary(enc)) = col {
            assert_eq!(
                meta,
                super::MetaData::<String> {
                    size: 301,
                    rows: 2,
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
            assert_eq!(meta.size, 40); // 4 i32s (16b) and a vec (24b)
            assert_eq!(meta.rows, 4);
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
            assert_eq!(meta.size, 32); // 4 u16s (8b) and a vec (24b)
            assert_eq!(meta.rows, 4);
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

        let hello = "hello".to_string();
        let world = "world".to_string();
        let mut exp = BTreeSet::new();
        exp.insert(Some(&hello));
        exp.insert(Some(&world));
        exp.insert(None);

        let col = Column::from(&input[..]);
        assert_eq!(col.distinct_values(&[0, 1, 2, 3, 4]), ValueSet::String(exp));
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
            EncodedValues::U32(vec![1, dictionary::NULL_ID, 2, 1, 2])
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
            EncodedValues::U32(vec![1, dictionary::NULL_ID, 2, 1, 2])
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
    fn aggregate_result() {
        let mut res = AggregateResult::Count(0);
        res.update(Value::Null);
        assert!(matches!(res, AggregateResult::Count(0)));
        res.update(Value::String("hello"));
        assert!(matches!(res, AggregateResult::Count(1)));

        let mut res = AggregateResult::Min(Value::Null);
        res.update(Value::String("Dance Yrself Clean"));
        assert!(matches!(
            res,
            AggregateResult::Min(Value::String("Dance Yrself Clean"))
        ));
        res.update(Value::String("All My Friends"));
        assert!(matches!(
            res,
            AggregateResult::Min(Value::String("All My Friends"))
        ));
        res.update(Value::String("Dance Yrself Clean"));
        assert!(matches!(
            res,
            AggregateResult::Min(Value::String("All My Friends"))
        ));
        res.update(Value::Null);
        assert!(matches!(
            res,
            AggregateResult::Min(Value::String("All My Friends"))
        ));

        let mut res = AggregateResult::Max(Value::Null);
        res.update(Value::Scalar(Scalar::I64(20)));
        assert!(matches!(
            res,
            AggregateResult::Max(Value::Scalar(Scalar::I64(20)))
        ));
        res.update(Value::Scalar(Scalar::I64(39)));
        assert!(matches!(
            res,
            AggregateResult::Max(Value::Scalar(Scalar::I64(39)))
        ));
        res.update(Value::Scalar(Scalar::I64(20)));
        assert!(matches!(
            res,
            AggregateResult::Max(Value::Scalar(Scalar::I64(39)))
        ));
        res.update(Value::Null);
        assert!(matches!(
            res,
            AggregateResult::Max(Value::Scalar(Scalar::I64(39)))
        ));

        let mut res = AggregateResult::Sum(Scalar::Null);
        res.update(Value::Null);
        assert!(matches!(res, AggregateResult::Sum(Scalar::Null)));
        res.update(Value::Scalar(Scalar::Null));
        assert!(matches!(res, AggregateResult::Sum(Scalar::Null)));

        res.update(Value::Scalar(Scalar::I64(20)));
        assert!(matches!(res, AggregateResult::Sum(Scalar::I64(20))));

        res.update(Value::Scalar(Scalar::I64(-5)));
        assert!(matches!(res, AggregateResult::Sum(Scalar::I64(15))));

        res.update(Value::Scalar(Scalar::Null));
        assert!(matches!(res, AggregateResult::Sum(Scalar::I64(15))));
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
