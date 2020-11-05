pub mod cmp;
pub mod dictionary;
pub mod fixed;
pub mod fixed_null;

use std::collections::BTreeSet;
use std::convert::TryFrom;

use croaring::Bitmap;

use arrow_deps::arrow::array::{
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
    UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow_deps::{arrow, arrow::array::Array};

/// The possible logical types that column values can have. All values in a
/// column have the same physical type.
pub enum Column {
    // A column of dictionary run-length encoded values.
    String(MetaData<String>, StringEncoding),

    // A column of single or double-precision floating point values.
    Float(MetaData<f64>, FloatEncoding),

    // A column of signed integers, which may be encoded with a different
    // physical type to the logical type.
    //
    // TODO - meta stored at highest precision, but returning correct logical
    // type probably needs some thought.
    Integer(MetaData<i64>, IntegerEncoding),

    // A column of unsigned integers, which may be encoded with a different
    // physical type to the logical type.
    //
    // TODO - meta stored at highest precision, but returning correct logical
    // type probably needs some thought.
    Unsigned(MetaData<u64>, IntegerEncoding), // TODO - 64-bit unsigned integers

    // These are TODO
    Bool,                                         // TODO - booleans
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
            Column::Bool => todo!(),
            Column::ByteArray(meta, _) => meta.rows,
        }
    }

    pub fn size(&self) -> u64 {
        todo!()
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
            format!(
                "cannot read row {:?} from column with {:?} rows",
                row_id,
                self.num_rows()
            )
        );

        match &self {
            Column::String(_, data) => data.value(row_id),
            Column::Float(_, data) => data.value(row_id),
            Column::Integer(_, data) => data.value(row_id),
            Column::Unsigned(_, data) => data.value(row_id),
            Column::Bool => todo!(),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    /// All values present at the provided logical row ids.
    pub fn values(&self, row_ids: &[u32]) -> Values {
        assert!(
            row_ids.len() as u32 <= self.num_rows(),
            format!(
                "too many row ids {:?} provided for column with {:?} rows",
                row_ids.len(),
                self.num_rows()
            )
        );

        match &self {
            Column::String(_, data) => data.values(row_ids),
            Column::Float(_, data) => data.values(row_ids),
            Column::Integer(_, data) => data.values(row_ids),
            Column::Unsigned(_, data) => data.values(row_ids),
            Column::Bool => todo!(),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    // The distinct set of values found at the logical row ids.
    pub fn distinct_values(&self, row_ids: &[u32]) -> ValueSet<'_> {
        assert!(
            row_ids.len() as u32 <= self.num_rows(),
            format!(
                "too many row ids {:?} provided for column with {:?} rows",
                row_ids.len(),
                self.num_rows()
            )
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
            format!(
                "too many row ids {:?} provided for column with {:?} rows",
                row_ids.len(),
                self.num_rows()
            )
        );

        match &self {
            Self::String(_, data) => match dst {
                EncodedValues::U32(dst) => EncodedValues::U32(data.encoded_values(row_ids, dst)),
                _ => unimplemented!("column type does not support requested encoding"),
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
            // are Tag Columns; they are `String` columns.
            _ => unimplemented!("encoded values on other column types not supported"),
        }
    }

    //
    // Methods for filtering
    //

    /// Determine the set of row ids that satisfy the predicate.
    ///
    /// TODO(edd): row ids pooling.
    pub fn row_ids_filter(&self, op: cmp::Operator, value: Value<'_>) -> RowIDsOption {
        // If we can get an answer using only the meta-data on the column then
        // return that answer.
        match self.evaluate_predicate_on_meta(&op, &value) {
            PredicateMatch::None => return RowIDsOption::None,
            PredicateMatch::All => return RowIDsOption::All,
            PredicateMatch::SomeMaybe => {} // have to apply predicate to column
        }

        // TODO(edd): figure out pooling of these
        let dst = RowIDs::Bitmap(Bitmap::create());

        // Check the column for all rows that satisfy the predicate.
        let row_ids = match &self {
            Column::String(_, data) => data.row_ids_filter(op, value.string().as_str(), dst),
            Column::Float(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Column::Integer(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Column::Unsigned(_, data) => data.row_ids_filter(op, value.scalar(), dst),
            Column::Bool => todo!(),
            Column::ByteArray(_, data) => todo!(),
        };

        if row_ids.is_empty() {
            return RowIDsOption::None;
        }
        RowIDsOption::Some(row_ids)
    }

    /// Determine the set of row ids that satisfy both of the predicates.
    ///
    /// Note: this method is a special case for common range-based predicates
    /// that are often found on timestamp columns.
    pub fn row_ids_filter_range(
        &self,
        low: (cmp::Operator, Value<'_>),
        high: (cmp::Operator, Value<'_>),
    ) -> RowIDsOption {
        let l = self.evaluate_predicate_on_meta(&low.0, &low.1);
        let h = self.evaluate_predicate_on_meta(&high.0, &high.1);
        match (l, h) {
            (PredicateMatch::All, PredicateMatch::All) => return RowIDsOption::All,

            // One of the predicates can't be satisfied, therefore no rows will
            // match both predicates.
            (PredicateMatch::None, _) | (_, PredicateMatch::None) => return RowIDsOption::None,

            // One of the predicates matches all rows so reduce the operation
            // to the other side.
            (PredicateMatch::SomeMaybe, PredicateMatch::All) => {
                return self.row_ids_filter(low.0, low.1);
            }
            (PredicateMatch::All, PredicateMatch::SomeMaybe) => {
                return self.row_ids_filter(high.0, high.1);
            }

            // Have to apply the predicates to the column to identify correct
            // set of rows.
            (PredicateMatch::SomeMaybe, PredicateMatch::SomeMaybe) => {}
        }

        // TODO(edd): figure out pooling of these
        let dst = RowIDs::Bitmap(Bitmap::create());

        // Check the column for all rows that satisfy the predicate.
        let row_ids = match &self {
            Column::String(_, data) => unimplemented!("not supported on string columns yet"),
            Column::Float(_, data) => {
                data.row_ids_filter_range((low.0, low.1.scalar()), (high.0, high.1.scalar()), dst)
            }
            Column::Integer(_, data) => {
                data.row_ids_filter_range((low.0, low.1.scalar()), (high.0, high.1.scalar()), dst)
            }
            Column::Unsigned(_, data) => {
                data.row_ids_filter_range((low.0, low.1.scalar()), (high.0, high.1.scalar()), dst)
            }
            Column::Bool => todo!(),
            Column::ByteArray(_, data) => todo!(),
        };

        if row_ids.is_empty() {
            return RowIDsOption::None;
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

    // Helper method to determine if the column possibly contains this value
    fn might_contain_value(&self, value: &Value<'_>) -> bool {
        match &self {
            Column::String(meta, _) => {
                if let Value::String(other) = value {
                    meta.might_contain_value(&other)
                } else {
                    unreachable!("impossible value comparison");
                }
            }
            // breaking this down:
            //   * Extract a Scalar variant from `value`, which should panic if
            //     that's not possible;
            //   * Try to safely convert that scalar to a primitive value based
            //     on the logical type used for the metadata on the column.
            //   * If the value can't be safely converted then there is no way
            //     that said value could be stored in the column at all -> false.
            //   * Otherwise if the value falls inside the range of values in
            //     the column range then it may be in the column -> true.
            Column::Float(meta, _) => value
                .scalar()
                .try_as_f64()
                .map_or_else(|| false, |v| meta.might_contain_value(&v)),
            Column::Integer(meta, _) => value
                .scalar()
                .try_as_i64()
                .map_or_else(|| false, |v| meta.might_contain_value(&v)),
            Column::Unsigned(meta, _) => value
                .scalar()
                .try_as_u64()
                .map_or_else(|| false, |v| meta.might_contain_value(&v)),
            Column::Bool => todo!(),
            Column::ByteArray(meta, _) => todo!(),
        }
    }

    // Helper method to determine if the predicate matches all the values in
    // the column.
    fn predicate_matches_all_values(&self, op: &cmp::Operator, value: &Value<'_>) -> bool {
        match &self {
            Column::String(meta, data) => {
                if data.contains_null() {
                    false
                } else if let Value::String(other) = value {
                    meta.might_match_all_values(op, other)
                } else {
                    unreachable!("impossible value comparison");
                }
            }
            // breaking this down:
            //   * If the column contains null values then it's not possible for
            //     all values in the column to match the predicate.
            //   * Extract a Scalar variant from `value`, which should panic if
            //     that's not possible;
            //   * Try to safely convert that scalar to a primitive value based
            //     on the logical type used for the metadata on the column.
            //   * If the value can't be safely converted then -> false.
            //   * Otherwise if the value falls inside the range of values in
            //     the column range then check if all values satisfy the
            //     predicate.
            //
            Column::Float(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_f64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, &v))
            }
            Column::Integer(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_i64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, &v))
            }
            Column::Unsigned(meta, data) => {
                if data.contains_null() {
                    return false;
                }

                value
                    .scalar()
                    .try_as_u64()
                    .map_or_else(|| false, |v| meta.might_match_all_values(op, &v))
            }
            Column::Bool => todo!(),
            Column::ByteArray(meta, _) => todo!(),
        }
    }

    // Helper method to determine if the predicate can not possibly match any
    // values in the column.
    fn predicate_matches_no_values(&self, op: &cmp::Operator, value: &Value<'_>) -> bool {
        match &self {
            Column::String(meta, data) => {
                if let Value::String(other) = value {
                    meta.match_no_values(op, other)
                } else {
                    unreachable!("impossible value comparison");
                }
            }
            // breaking this down:
            //   * Extract a Scalar variant from `value`, which should panic if
            //     that's not possible;
            //   * Convert that scalar to a primitive value based
            //     on the logical type used for the metadata on the column.
            //   * See if one can prove none of the column can match the predicate.
            //
            Column::Float(meta, data) => meta.match_no_values(op, &value.scalar().as_f64()),
            Column::Integer(meta, data) => meta.match_no_values(op, &value.scalar().as_i64()),
            Column::Unsigned(meta, data) => meta.match_no_values(op, &value.scalar().as_u64()),
            Column::Bool => todo!(),
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
            Column::Bool => todo!(),
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
            Column::Bool => todo!(),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    //
    // Methods for aggregating
    //

    /// The summation of all non-null values located at the provided rows.
    pub fn sum(&self, row_ids: &[u32]) -> Value<'_> {
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
            Column::Bool => todo!(),
            Column::ByteArray(_, _) => todo!(),
        }
    }

    //
    // Methods for inspecting
    //

    /// Determines if the column has a non-null value at any of the provided rows.
    pub fn has_non_null_value(&self, row_ids: &[u32]) -> bool {
        todo!()
    }

    /// Determines if the column contains other values than those provided in
    /// `values`.
    pub fn contains_other_values(&self, values: &BTreeSet<Option<&String>>) -> bool {
        todo!()
    }
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
}

impl<T: PartialOrd + std::fmt::Debug> MetaData<T> {
    fn might_contain_value(&self, v: &T) -> bool {
        match &self.range {
            Some(range) => &range.0 <= v && v <= &range.1,
            None => false,
        }
    }

    // Determines if it's possible that predicate would match all rows in the
    // column. It is up to the caller to determine if the column contains null
    // values, which would invalidate a truthful result.
    fn might_match_all_values(&self, op: &cmp::Operator, v: &T) -> bool {
        match &self.range {
            Some(range) => match op {
                // all values in column equal to v
                cmp::Operator::Equal => range.0 == range.1 && &range.1 == v,
                // all values larger or smaller than v so can't contain v
                cmp::Operator::NotEqual => v < &range.0 || v > &range.1,
                // all values in column > v
                cmp::Operator::GT => &range.0 > v,
                // all values in column >= v
                cmp::Operator::GTE => &range.0 >= v,
                // all values in column < v
                cmp::Operator::LT => &range.1 < v,
                // all values in column <= v
                cmp::Operator::LTE => &range.1 <= v,
            },
            None => false, // only null values in column.
        }
    }

    // Determines if it can be shown that the predicate would not match any rows
    // in the column.
    fn match_no_values(&self, op: &cmp::Operator, v: &T) -> bool {
        match &self.range {
            Some(range) => match op {
                // no values are `v` so no rows will match `== v`
                cmp::Operator::Equal => range.0 == range.1 && &range.1 != v,
                // all values are `v` so no rows will match `!= v`
                cmp::Operator::NotEqual => range.0 == range.1 && &range.1 == v,
                // max value in column is `<= v` so no values can be `> v`
                cmp::Operator::GT => &range.1 <= v,
                // max value in column is `< v` so no values can be `>= v`
                cmp::Operator::GTE => &range.1 < v,
                // min value in column is `>= v` so no values can be `< v`
                cmp::Operator::LT => &range.0 >= v,
                // min value in column is `> v` so no values can be `<= v`
                cmp::Operator::LTE => &range.0 > v,
            },
            None => true, // only null values in column so no values satisfy `v`
        }
    }
}
pub enum StringEncoding {
    RLE(dictionary::RLE),
    // TODO - simple array encoding, e.g., via Arrow String array.
}

/// This implementation is concerned with how to produce string columns with
/// different encodings.
impl StringEncoding {
    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        match &self {
            Self::RLE(c) => c.contains_null(),
        }
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            Self::RLE(c) => match c.value(row_id) {
                Some(v) => Value::String(v),
                None => Value::Null,
            },
        }
    }

    /// All values present at the provided logical row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values {
        match &self {
            Self::RLE(c) => Values::String(StringArray::from(c.values(row_ids, vec![]))),
        }
    }

    /// Returns the distinct set of values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination sets.
    pub fn distinct_values(&self, row_ids: &[u32]) -> ValueSet<'_> {
        match &self {
            Self::RLE(c) => ValueSet::String(c.distinct_values(row_ids, BTreeSet::new())),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    pub fn row_ids_filter(&self, op: cmp::Operator, value: &str, dst: RowIDs) -> RowIDs {
        match &self {
            Self::RLE(c) => c.row_ids_filter(value, op, dst),
        }
    }

    /// The lexicographic minimum non-null value at the rows specified, or the
    /// NULL value if the column only contains NULL values at the provided row
    /// ids.
    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            StringEncoding::RLE(c) => match c.min(row_ids) {
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
            StringEncoding::RLE(c) => match c.max(row_ids) {
                Some(max) => Value::String(max),
                None => Value::Null,
            },
        }
    }

    /// The number of non-null values at the provided row ids.
    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            StringEncoding::RLE(c) => c.count(row_ids),
        }
    }

    fn from_arrow_string_array(arr: arrow::array::StringArray) -> Self {
        //
        // TODO(edd): potentially switch on things like cardinality in the input
        // and encode in different ways. Right now we only encode with RLE.
        //

        // RLE creation.

        // build a sorted dictionary.
        let mut dictionary = BTreeSet::new();

        for i in 0..arr.len() {
            if !arr.is_null(i) {
                dictionary.insert(arr.value(i).to_string());
            }
        }

        let mut data = dictionary::RLE::with_dictionary(dictionary);

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
            size: 0,
            rows: data.num_rows(),
            range,
        };

        Self::RLE(data)
    }

    /// All encoded values for the provided logical row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn encoded_values(&self, row_ids: &[u32], dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RLE(c) => c.encoded_values(row_ids, dst),
        }
    }

    /// All encoded values for the column.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn all_encoded_values(&self, dst: Vec<u32>) -> Vec<u32> {
        match &self {
            Self::RLE(c) => c.all_encoded_values(dst),
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

        Self::RLE(data)
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

        Self::RLE(data)
    }

    // generates metadata for an encoded column.
    fn meta(data: &Self) -> MetaData<String> {
        match data {
            StringEncoding::RLE(data) => {
                let dictionary = data.dictionary();
                let range = if !dictionary.is_empty() {
                    let min = data.dictionary()[0].clone();
                    let max = data.dictionary()[data.dictionary().len() - 1].clone();
                    Some((min, max))
                } else {
                    None
                };

                MetaData {
                    size: 0,
                    rows: data.num_rows(),
                    range,
                }
            }
        }
    }
}

pub enum IntegerEncoding {
    I64I64(fixed::Fixed<i64>),
    I64I32(fixed::Fixed<i32>),
    I64U32(fixed::Fixed<u32>),
    I64I16(fixed::Fixed<i16>),
    I64U16(fixed::Fixed<u16>),
    I64I8(fixed::Fixed<i8>),
    I64U8(fixed::Fixed<u8>),
    I32I32(fixed::Fixed<i32>),
    I32I16(fixed::Fixed<i16>),
    I32U16(fixed::Fixed<u16>),
    I32I8(fixed::Fixed<i8>),
    I32U8(fixed::Fixed<u8>),
    I16I16(fixed::Fixed<i16>),
    I16I8(fixed::Fixed<i8>),
    I16U8(fixed::Fixed<u8>),
    I8I8(fixed::Fixed<i8>),

    U64U64(fixed::Fixed<u64>),
    U64U32(fixed::Fixed<u32>),
    U64U16(fixed::Fixed<u16>),
    U64U8(fixed::Fixed<u8>),
    U32U32(fixed::Fixed<u32>),
    U32U16(fixed::Fixed<u16>),
    U32U8(fixed::Fixed<u8>),
    U16U16(fixed::Fixed<u16>),
    U16U8(fixed::Fixed<u8>),
    U8U8(fixed::Fixed<u8>),

    // TODO - add all the other possible integer combinations.

    // Nullable encodings - TODO
    I64I64N(fixed_null::FixedNull<arrow::datatypes::Int64Type>),
}

impl IntegerEncoding {
    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        if let Self::I64I64N(c) = &self {
            return c.contains_null();
        }
        false
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            // N.B., The `Scalar` variant determines the physical type `U` that
            // `c.value` should return as the logical type

            // signed 64-bit variants - logical type is i64 for all these
            Self::I64I64(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64I32(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64U32(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64I16(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64U16(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64I8(c) => Value::Scalar(Scalar::I64(c.value(row_id))),
            Self::I64U8(c) => Value::Scalar(Scalar::I64(c.value(row_id))),

            // signed 32-bit variants - logical type is i32 for all these
            Self::I32I32(c) => Value::Scalar(Scalar::I32(c.value(row_id))),
            Self::I32I16(c) => Value::Scalar(Scalar::I32(c.value(row_id))),
            Self::I32U16(c) => Value::Scalar(Scalar::I32(c.value(row_id))),
            Self::I32I8(c) => Value::Scalar(Scalar::I32(c.value(row_id))),
            Self::I32U8(c) => Value::Scalar(Scalar::I64(c.value(row_id))),

            // signed 16-bit variants - logical type is i16 for all these
            Self::I16I16(c) => Value::Scalar(Scalar::I16(c.value(row_id))),
            Self::I16I8(c) => Value::Scalar(Scalar::I16(c.value(row_id))),
            Self::I16U8(c) => Value::Scalar(Scalar::I16(c.value(row_id))),

            // signed 8-bit variant - logical type is i8
            Self::I8I8(c) => Value::Scalar(Scalar::I8(c.value(row_id))),

            // unsigned 64-bit variants - logical type is u64 for all these
            Self::U64U64(c) => Value::Scalar(Scalar::U64(c.value(row_id))),
            Self::U64U32(c) => Value::Scalar(Scalar::U64(c.value(row_id))),
            Self::U64U16(c) => Value::Scalar(Scalar::U64(c.value(row_id))),
            Self::U64U8(c) => Value::Scalar(Scalar::U64(c.value(row_id))),

            // unsigned 32-bit variants - logical type is u32 for all these
            Self::U32U32(c) => Value::Scalar(Scalar::U32(c.value(row_id))),
            Self::U32U16(c) => Value::Scalar(Scalar::U32(c.value(row_id))),
            Self::U32U8(c) => Value::Scalar(Scalar::U32(c.value(row_id))),

            // unsigned 16-bit variants - logical type is u16 for all these
            Self::U16U16(c) => Value::Scalar(Scalar::U16(c.value(row_id))),
            Self::U16U8(c) => Value::Scalar(Scalar::U16(c.value(row_id))),

            // unsigned 8-bit variant - logical type is u8
            Self::U8U8(c) => Value::Scalar(Scalar::U8(c.value(row_id))),

            Self::I64I64N(c) => match c.value(row_id) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
        }
    }

    /// Returns the logical values found at the provided row ids.
    ///
    /// TODO(edd): perf - provide a pooling mechanism for these destination vectors
    /// so that they can be re-used.
    pub fn values(&self, row_ids: &[u32]) -> Values {
        match &self {
            // signed 64-bit variants - logical type is i64 for all these
            Self::I64I64(c) => Values::I64(Int64Array::from(c.values::<i64>(row_ids, vec![]))),
            Self::I64I32(c) => Values::I64(Int64Array::from(c.values::<i64>(row_ids, vec![]))),
            Self::I64U32(c) => Values::I64(Int64Array::from(c.values::<i64>(row_ids, vec![]))),
            Self::I64I16(c) => Values::I64(Int64Array::from(c.values::<i64>(row_ids, vec![]))),
            Self::I64U16(c) => Values::I64(Int64Array::from(c.values::<i64>(row_ids, vec![]))),
            Self::I64I8(c) => Values::I64(Int64Array::from(c.values::<i64>(row_ids, vec![]))),
            Self::I64U8(c) => Values::I64(Int64Array::from(c.values::<i64>(row_ids, vec![]))),

            // signed 32-bit variants - logical type is i32 for all these
            Self::I32I32(c) => Values::I32(Int32Array::from(c.values::<i32>(row_ids, vec![]))),
            Self::I32I16(c) => Values::I32(Int32Array::from(c.values::<i32>(row_ids, vec![]))),
            Self::I32U16(c) => Values::I32(Int32Array::from(c.values::<i32>(row_ids, vec![]))),
            Self::I32I8(c) => Values::I32(Int32Array::from(c.values::<i32>(row_ids, vec![]))),
            Self::I32U8(c) => Values::I32(Int32Array::from(c.values::<i32>(row_ids, vec![]))),

            // signed 16-bit variants - logical type is i16 for all these
            Self::I16I16(c) => Values::I16(Int16Array::from(c.values::<i16>(row_ids, vec![]))),
            Self::I16I8(c) => Values::I16(Int16Array::from(c.values::<i16>(row_ids, vec![]))),
            Self::I16U8(c) => Values::I16(Int16Array::from(c.values::<i16>(row_ids, vec![]))),

            // signed 8-bit variant - logical type is i8
            Self::I8I8(c) => Values::I8(Int8Array::from(c.values::<i8>(row_ids, vec![]))),

            // unsigned 64-bit variants - logical type is u64 for all these
            Self::U64U64(c) => Values::U64(UInt64Array::from(c.values::<u64>(row_ids, vec![]))),
            Self::U64U32(c) => Values::U64(UInt64Array::from(c.values::<u64>(row_ids, vec![]))),
            Self::U64U16(c) => Values::U64(UInt64Array::from(c.values::<u64>(row_ids, vec![]))),
            Self::U64U8(c) => Values::U64(UInt64Array::from(c.values::<u64>(row_ids, vec![]))),

            // unsigned 32-bit variants - logical type is u32 for all these
            Self::U32U32(c) => Values::U32(UInt32Array::from(c.values::<u32>(row_ids, vec![]))),
            Self::U32U16(c) => Values::U32(UInt32Array::from(c.values::<u32>(row_ids, vec![]))),
            Self::U32U8(c) => Values::U32(UInt32Array::from(c.values::<u32>(row_ids, vec![]))),

            // unsigned 16-bit variants - logical type is u16 for all these
            Self::U16U16(c) => Values::U16(UInt16Array::from(c.values::<u16>(row_ids, vec![]))),
            Self::U16U8(c) => Values::U16(UInt16Array::from(c.values::<u16>(row_ids, vec![]))),

            // unsigned 8-bit variant - logical type is u8
            Self::U8U8(c) => Values::U8(UInt8Array::from(c.values::<u8>(row_ids, vec![]))),

            Self::I64I64N(c) => Values::I64(Int64Array::from(c.values(row_ids, vec![]))),
        }
    }

    /// Returns the encoded values found at the provided row ids. For an
    /// `IntegerEncoding` the encoded values are typically just the raw values.
    pub fn encoded_values(&self, row_ids: &[u32], dst: EncodedValues) -> EncodedValues {
        // Right now the use-case for encoded values on non-string columns is
        // that it's used for grouping with timestamp columns, which should be
        // non-null signed 64-bit integers.
        match dst {
            EncodedValues::I64(dst) => match &self {
                Self::I64I64(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64I32(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64U32(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64I16(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64U16(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64I8(data) => EncodedValues::I64(data.values(row_ids, dst)),
                Self::I64U8(data) => EncodedValues::I64(data.values(row_ids, dst)),
                _ => unreachable!("encoded values on encoding type not supported"),
            },
            _ => unreachable!("currently only support encoded values as i64"),
        }
    }

    /// All encoded values for the column. For `IntegerEncoding` this is
    /// typically equivalent to `all_values`.
    pub fn all_encoded_values(&self, dst: EncodedValues) -> EncodedValues {
        // Right now the use-case for encoded values on non-string columns is
        // that it's used for grouping with timestamp columns, which should be
        // non-null signed 64-bit integers.
        match dst {
            EncodedValues::I64(dst) => match &self {
                Self::I64I64(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64I32(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64U32(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64I16(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64U16(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64I8(data) => EncodedValues::I64(data.all_values(dst)),
                Self::I64U8(data) => EncodedValues::I64(data.all_values(dst)),
                _ => unreachable!("encoded values on encoding type not supported"),
            },
            _ => unreachable!("currently only support encoded values as i64"),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: cmp::Operator, value: &Scalar, dst: RowIDs) -> RowIDs {
        match &self {
            Self::I64I64(c) => c.row_ids_filter(value.as_i64(), op, dst),
            Self::I64I32(c) => c.row_ids_filter(value.as_i32(), op, dst),
            Self::I64U32(c) => c.row_ids_filter(value.as_u32(), op, dst),
            Self::I64I16(c) => c.row_ids_filter(value.as_i16(), op, dst),
            Self::I64U16(c) => c.row_ids_filter(value.as_u16(), op, dst),
            Self::I64I8(c) => c.row_ids_filter(value.as_i8(), op, dst),
            Self::I64U8(c) => c.row_ids_filter(value.as_u8(), op, dst),
            Self::I32I32(c) => c.row_ids_filter(value.as_i32(), op, dst),
            Self::I32I16(c) => c.row_ids_filter(value.as_i16(), op, dst),
            Self::I32U16(c) => c.row_ids_filter(value.as_u16(), op, dst),
            Self::I32I8(c) => c.row_ids_filter(value.as_i8(), op, dst),
            Self::I32U8(c) => c.row_ids_filter(value.as_u8(), op, dst),
            Self::I16I16(c) => c.row_ids_filter(value.as_i16(), op, dst),
            Self::I16I8(c) => c.row_ids_filter(value.as_i8(), op, dst),
            Self::I16U8(c) => c.row_ids_filter(value.as_u8(), op, dst),
            Self::I8I8(c) => c.row_ids_filter(value.as_i8(), op, dst),
            Self::U64U64(c) => c.row_ids_filter(value.as_u64(), op, dst),
            Self::U64U32(c) => c.row_ids_filter(value.as_u32(), op, dst),
            Self::U64U16(c) => c.row_ids_filter(value.as_u16(), op, dst),
            Self::U64U8(c) => c.row_ids_filter(value.as_u8(), op, dst),
            Self::U32U32(c) => c.row_ids_filter(value.as_u32(), op, dst),
            Self::U32U16(c) => c.row_ids_filter(value.as_u16(), op, dst),
            Self::U32U8(c) => c.row_ids_filter(value.as_u8(), op, dst),
            Self::U16U16(c) => c.row_ids_filter(value.as_u16(), op, dst),
            Self::U16U8(c) => c.row_ids_filter(value.as_u8(), op, dst),
            Self::U8U8(c) => c.row_ids_filter(value.as_u8(), op, dst),
            Self::I64I64N(c) => c.row_ids_filter(value.as_i64(), op, dst),
        }
    }

    /// Returns the row ids that satisfy both the provided predicates.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter_range(
        &self,
        low: (cmp::Operator, &Scalar),
        high: (cmp::Operator, &Scalar),
        dst: RowIDs,
    ) -> RowIDs {
        match &self {
            Self::I64I64(c) => {
                c.row_ids_filter_range((low.1.as_i64(), low.0), (high.1.as_i64(), high.0), dst)
            }
            Self::I64I32(c) => {
                c.row_ids_filter_range((low.1.as_i32(), low.0), (high.1.as_i32(), high.0), dst)
            }
            Self::I64U32(c) => {
                c.row_ids_filter_range((low.1.as_u32(), low.0), (high.1.as_u32(), high.0), dst)
            }
            Self::I64I16(c) => {
                c.row_ids_filter_range((low.1.as_i16(), low.0), (high.1.as_i16(), high.0), dst)
            }
            Self::I64U16(c) => {
                c.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::I64I8(c) => {
                c.row_ids_filter_range((low.1.as_i8(), low.0), (high.1.as_i8(), high.0), dst)
            }
            Self::I64U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::I32I32(c) => {
                c.row_ids_filter_range((low.1.as_i32(), low.0), (high.1.as_i32(), high.0), dst)
            }
            Self::I32I16(c) => {
                c.row_ids_filter_range((low.1.as_i16(), low.0), (high.1.as_i16(), high.0), dst)
            }
            Self::I32U16(c) => {
                c.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::I32I8(c) => {
                c.row_ids_filter_range((low.1.as_i8(), low.0), (high.1.as_i8(), high.0), dst)
            }
            Self::I32U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::I16I16(c) => {
                c.row_ids_filter_range((low.1.as_i16(), low.0), (high.1.as_i16(), high.0), dst)
            }
            Self::I16I8(c) => {
                c.row_ids_filter_range((low.1.as_i8(), low.0), (high.1.as_i8(), high.0), dst)
            }
            Self::I16U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::I8I8(c) => {
                c.row_ids_filter_range((low.1.as_i8(), low.0), (high.1.as_i8(), high.0), dst)
            }
            Self::U64U64(c) => {
                c.row_ids_filter_range((low.1.as_u64(), low.0), (high.1.as_u64(), high.0), dst)
            }
            Self::U64U32(c) => {
                c.row_ids_filter_range((low.1.as_u32(), low.0), (high.1.as_u32(), high.0), dst)
            }
            Self::U64U16(c) => {
                c.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::U64U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::U32U32(c) => {
                c.row_ids_filter_range((low.1.as_u32(), low.0), (high.1.as_u32(), high.0), dst)
            }
            Self::U32U16(c) => {
                c.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::U32U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::U16U16(c) => {
                c.row_ids_filter_range((low.1.as_u16(), low.0), (high.1.as_u16(), high.0), dst)
            }
            Self::U16U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::U8U8(c) => {
                c.row_ids_filter_range((low.1.as_u8(), low.0), (high.1.as_u8(), high.0), dst)
            }
            Self::I64I64N(c) => todo!(),
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            IntegerEncoding::I64I64(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            IntegerEncoding::I64I32(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            IntegerEncoding::I64U32(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            IntegerEncoding::I64I16(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            IntegerEncoding::I64U16(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            IntegerEncoding::I64I8(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            IntegerEncoding::I64U8(c) => Value::Scalar(Scalar::I64(c.min(row_ids))),
            IntegerEncoding::I32I32(c) => Value::Scalar(Scalar::I32(c.min(row_ids))),
            IntegerEncoding::I32I16(c) => Value::Scalar(Scalar::I32(c.min(row_ids))),
            IntegerEncoding::I32U16(c) => Value::Scalar(Scalar::I32(c.min(row_ids))),
            IntegerEncoding::I32I8(c) => Value::Scalar(Scalar::I32(c.min(row_ids))),
            IntegerEncoding::I32U8(c) => Value::Scalar(Scalar::I32(c.min(row_ids))),
            IntegerEncoding::I16I16(c) => Value::Scalar(Scalar::I16(c.min(row_ids))),
            IntegerEncoding::I16I8(c) => Value::Scalar(Scalar::I16(c.min(row_ids))),
            IntegerEncoding::I16U8(c) => Value::Scalar(Scalar::I16(c.min(row_ids))),
            IntegerEncoding::I8I8(c) => Value::Scalar(Scalar::I8(c.min(row_ids))),
            IntegerEncoding::U64U64(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            IntegerEncoding::U64U32(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            IntegerEncoding::U64U16(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            IntegerEncoding::U64U8(c) => Value::Scalar(Scalar::U64(c.min(row_ids))),
            IntegerEncoding::U32U32(c) => Value::Scalar(Scalar::U32(c.min(row_ids))),
            IntegerEncoding::U32U16(c) => Value::Scalar(Scalar::U32(c.min(row_ids))),
            IntegerEncoding::U32U8(c) => Value::Scalar(Scalar::U32(c.min(row_ids))),
            IntegerEncoding::U16U16(c) => Value::Scalar(Scalar::U16(c.min(row_ids))),
            IntegerEncoding::U16U8(c) => Value::Scalar(Scalar::U16(c.min(row_ids))),
            IntegerEncoding::U8U8(c) => Value::Scalar(Scalar::U8(c.min(row_ids))),
            IntegerEncoding::I64I64N(c) => match c.min(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            IntegerEncoding::I64I64(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            IntegerEncoding::I64I32(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            IntegerEncoding::I64U32(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            IntegerEncoding::I64I16(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            IntegerEncoding::I64U16(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            IntegerEncoding::I64I8(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            IntegerEncoding::I64U8(c) => Value::Scalar(Scalar::I64(c.max(row_ids))),
            IntegerEncoding::I32I32(c) => Value::Scalar(Scalar::I32(c.max(row_ids))),
            IntegerEncoding::I32I16(c) => Value::Scalar(Scalar::I32(c.max(row_ids))),
            IntegerEncoding::I32U16(c) => Value::Scalar(Scalar::I32(c.max(row_ids))),
            IntegerEncoding::I32I8(c) => Value::Scalar(Scalar::I32(c.max(row_ids))),
            IntegerEncoding::I32U8(c) => Value::Scalar(Scalar::I32(c.max(row_ids))),
            IntegerEncoding::I16I16(c) => Value::Scalar(Scalar::I16(c.max(row_ids))),
            IntegerEncoding::I16I8(c) => Value::Scalar(Scalar::I16(c.max(row_ids))),
            IntegerEncoding::I16U8(c) => Value::Scalar(Scalar::I16(c.max(row_ids))),
            IntegerEncoding::I8I8(c) => Value::Scalar(Scalar::I8(c.max(row_ids))),
            IntegerEncoding::U64U64(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            IntegerEncoding::U64U32(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            IntegerEncoding::U64U16(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            IntegerEncoding::U64U8(c) => Value::Scalar(Scalar::U64(c.max(row_ids))),
            IntegerEncoding::U32U32(c) => Value::Scalar(Scalar::U32(c.max(row_ids))),
            IntegerEncoding::U32U16(c) => Value::Scalar(Scalar::U32(c.max(row_ids))),
            IntegerEncoding::U32U8(c) => Value::Scalar(Scalar::U32(c.max(row_ids))),
            IntegerEncoding::U16U16(c) => Value::Scalar(Scalar::U16(c.max(row_ids))),
            IntegerEncoding::U16U8(c) => Value::Scalar(Scalar::U16(c.max(row_ids))),
            IntegerEncoding::U8U8(c) => Value::Scalar(Scalar::U8(c.max(row_ids))),
            IntegerEncoding::I64I64N(c) => match c.max(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            IntegerEncoding::I64I64(c) => Value::Scalar(Scalar::I64(c.sum(row_ids))),
            IntegerEncoding::I64I32(c) => Value::Scalar(Scalar::I64(c.sum(row_ids))),
            IntegerEncoding::I64U32(c) => Value::Scalar(Scalar::I64(c.sum(row_ids))),
            IntegerEncoding::I64I16(c) => Value::Scalar(Scalar::I64(c.sum(row_ids))),
            IntegerEncoding::I64U16(c) => Value::Scalar(Scalar::I64(c.sum(row_ids))),
            IntegerEncoding::I64I8(c) => Value::Scalar(Scalar::I64(c.sum(row_ids))),
            IntegerEncoding::I64U8(c) => Value::Scalar(Scalar::I64(c.sum(row_ids))),
            IntegerEncoding::I32I32(c) => Value::Scalar(Scalar::I32(c.sum(row_ids))),
            IntegerEncoding::I32I16(c) => Value::Scalar(Scalar::I32(c.sum(row_ids))),
            IntegerEncoding::I32U16(c) => Value::Scalar(Scalar::I32(c.sum(row_ids))),
            IntegerEncoding::I32I8(c) => Value::Scalar(Scalar::I32(c.sum(row_ids))),
            IntegerEncoding::I32U8(c) => Value::Scalar(Scalar::I32(c.sum(row_ids))),
            IntegerEncoding::I16I16(c) => Value::Scalar(Scalar::I16(c.sum(row_ids))),
            IntegerEncoding::I16I8(c) => Value::Scalar(Scalar::I16(c.sum(row_ids))),
            IntegerEncoding::I16U8(c) => Value::Scalar(Scalar::I16(c.sum(row_ids))),
            IntegerEncoding::I8I8(c) => Value::Scalar(Scalar::I8(c.sum(row_ids))),
            IntegerEncoding::U64U64(c) => Value::Scalar(Scalar::U64(c.sum(row_ids))),
            IntegerEncoding::U64U32(c) => Value::Scalar(Scalar::U64(c.sum(row_ids))),
            IntegerEncoding::U64U16(c) => Value::Scalar(Scalar::U64(c.sum(row_ids))),
            IntegerEncoding::U64U8(c) => Value::Scalar(Scalar::U64(c.sum(row_ids))),
            IntegerEncoding::U32U32(c) => Value::Scalar(Scalar::U32(c.sum(row_ids))),
            IntegerEncoding::U32U16(c) => Value::Scalar(Scalar::U32(c.sum(row_ids))),
            IntegerEncoding::U32U8(c) => Value::Scalar(Scalar::U32(c.sum(row_ids))),
            IntegerEncoding::U16U16(c) => Value::Scalar(Scalar::U16(c.sum(row_ids))),
            IntegerEncoding::U16U8(c) => Value::Scalar(Scalar::U16(c.sum(row_ids))),
            IntegerEncoding::U8U8(c) => Value::Scalar(Scalar::U8(c.sum(row_ids))),
            IntegerEncoding::I64I64N(c) => match c.sum(row_ids) {
                Some(v) => Value::Scalar(Scalar::I64(v)),
                None => Value::Null,
            },
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            IntegerEncoding::I64I64(c) => c.count(row_ids),
            IntegerEncoding::I64I32(c) => c.count(row_ids),
            IntegerEncoding::I64U32(c) => c.count(row_ids),
            IntegerEncoding::I64I16(c) => c.count(row_ids),
            IntegerEncoding::I64U16(c) => c.count(row_ids),
            IntegerEncoding::I64I8(c) => c.count(row_ids),
            IntegerEncoding::I64U8(c) => c.count(row_ids),
            IntegerEncoding::I32I32(c) => c.count(row_ids),
            IntegerEncoding::I32I16(c) => c.count(row_ids),
            IntegerEncoding::I32U16(c) => c.count(row_ids),
            IntegerEncoding::I32I8(c) => c.count(row_ids),
            IntegerEncoding::I32U8(c) => c.count(row_ids),
            IntegerEncoding::I16I16(c) => c.count(row_ids),
            IntegerEncoding::I16I8(c) => c.count(row_ids),
            IntegerEncoding::I16U8(c) => c.count(row_ids),
            IntegerEncoding::I8I8(c) => c.count(row_ids),
            IntegerEncoding::U64U64(c) => c.count(row_ids),
            IntegerEncoding::U64U32(c) => c.count(row_ids),
            IntegerEncoding::U64U16(c) => c.count(row_ids),
            IntegerEncoding::U64U8(c) => c.count(row_ids),
            IntegerEncoding::U32U32(c) => c.count(row_ids),
            IntegerEncoding::U32U16(c) => c.count(row_ids),
            IntegerEncoding::U32U8(c) => c.count(row_ids),
            IntegerEncoding::U16U16(c) => c.count(row_ids),
            IntegerEncoding::U16U8(c) => c.count(row_ids),
            IntegerEncoding::U8U8(c) => c.count(row_ids),
            IntegerEncoding::I64I64N(c) => c.count(row_ids),
        }
    }
}

pub enum FloatEncoding {
    Fixed64(fixed::Fixed<f64>),
    Fixed32(fixed::Fixed<f32>),
    // TODO(edd): encodings for nullable columns
}

impl FloatEncoding {
    /// Determines if the column contains a NULL value.
    pub fn contains_null(&self) -> bool {
        // TODO(edd): when adding the nullable columns then ask the nullable
        // encoding if it has any null values.
        false
    }

    /// Returns the logical value found at the provided row id.
    pub fn value(&self, row_id: u32) -> Value<'_> {
        match &self {
            // N.B., The `Scalar` variant determines the physical type `U` that
            // `c.value` should return.
            Self::Fixed64(c) => Value::Scalar(Scalar::F64(c.value(row_id))),
            Self::Fixed32(c) => Value::Scalar(Scalar::F32(c.value(row_id))),
        }
    }

    /// Returns the logical values found at the provided row ids.
    ///
    /// TODO(edd): perf - pooling of destination vectors.
    pub fn values(&self, row_ids: &[u32]) -> Values {
        match &self {
            Self::Fixed64(c) => Values::F64(Float64Array::from(c.values::<f64>(row_ids, vec![]))),
            Self::Fixed32(c) => Values::F32(Float32Array::from(c.values::<f32>(row_ids, vec![]))),
        }
    }

    /// Returns the row ids that satisfy the provided predicate.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter(&self, op: cmp::Operator, value: &Scalar, dst: RowIDs) -> RowIDs {
        match &self {
            FloatEncoding::Fixed64(c) => c.row_ids_filter(value.as_f64(), op, dst),
            FloatEncoding::Fixed32(c) => c.row_ids_filter(value.as_f32(), op, dst),
        }
    }

    /// Returns the row ids that satisfy both the provided predicates.
    ///
    /// Note: it is the caller's responsibility to ensure that the provided
    /// `Scalar` value will fit within the physical type of the encoded column.
    /// `row_ids_filter` will panic if this invariant is broken.
    pub fn row_ids_filter_range(
        &self,
        low: (cmp::Operator, &Scalar),
        high: (cmp::Operator, &Scalar),
        dst: RowIDs,
    ) -> RowIDs {
        match &self {
            FloatEncoding::Fixed64(c) => {
                c.row_ids_filter_range((low.1.as_f64(), low.0), (high.1.as_f64(), high.0), dst)
            }
            FloatEncoding::Fixed32(c) => {
                c.row_ids_filter_range((low.1.as_f32(), low.0), (high.1.as_f32(), high.0), dst)
            }
        }
    }

    pub fn min(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            FloatEncoding::Fixed64(c) => Value::Scalar(Scalar::F64(c.min(row_ids))),
            FloatEncoding::Fixed32(c) => Value::Scalar(Scalar::F32(c.min(row_ids))),
        }
    }

    pub fn max(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            FloatEncoding::Fixed64(c) => Value::Scalar(Scalar::F64(c.max(row_ids))),
            FloatEncoding::Fixed32(c) => Value::Scalar(Scalar::F32(c.max(row_ids))),
        }
    }

    pub fn sum(&self, row_ids: &[u32]) -> Value<'_> {
        match &self {
            FloatEncoding::Fixed64(c) => Value::Scalar(Scalar::F64(c.sum(row_ids))),
            FloatEncoding::Fixed32(c) => Value::Scalar(Scalar::F32(c.sum(row_ids))),
        }
    }

    pub fn count(&self, row_ids: &[u32]) -> u32 {
        match &self {
            FloatEncoding::Fixed64(c) => c.count(row_ids),
            FloatEncoding::Fixed32(c) => c.count(row_ids),
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
        let data = StringEncoding::from_arrow_string_array(arr);
        Column::String(StringEncoding::meta(&data), data)
    }
}

impl From<&[Option<&str>]> for Column {
    fn from(arr: &[Option<&str>]) -> Self {
        let data = StringEncoding::from_opt_strs(arr);
        Column::String(StringEncoding::meta(&data), data)
    }
}

impl From<&[&str]> for Column {
    fn from(arr: &[&str]) -> Self {
        let data = StringEncoding::from_strs(arr);
        Column::String(StringEncoding::meta(&data), data)
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

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data type.
        match (min, max) {
            // encode as u8 values
            (min, max) if max <= u8::MAX as u64 => {
                let data = fixed::Fixed::<u8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Unsigned(meta, IntegerEncoding::U64U8(data))
            }
            // encode as u16 values
            (min, max) if max <= u16::MAX as u64 => {
                let data = fixed::Fixed::<u16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Unsigned(meta, IntegerEncoding::U64U16(data))
            }
            // encode as u32 values
            (min, max) if max <= u32::MAX as u64 => {
                let data = fixed::Fixed::<u32>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Unsigned(meta, IntegerEncoding::U64U32(data))
            }
            // otherwise, encode with the same physical type (u64)
            (_, _) => {
                let data = fixed::Fixed::<u64>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Unsigned(meta, IntegerEncoding::U64U64(data))
            }
        }
    }
}

/// Converts a slice of u32 values into the most compact fixed-width physical
/// encoding.
impl From<&[u32]> for Column {
    fn from(arr: &[u32]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data type.
        match (min, max) {
            // encode as u8 values
            (min, max) if max <= u8::MAX as u32 => {
                let data = fixed::Fixed::<u8>::from(arr);
                let meta = MetaData::<u64> {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as u64, max as u64)),
                };
                Column::Unsigned(meta, IntegerEncoding::U32U8(data))
            }
            // encode as u16 values
            (min, max) if max <= u16::MAX as u32 => {
                let data = fixed::Fixed::<u16>::from(arr);
                let meta = MetaData::<u64> {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as u64, max as u64)),
                };
                Column::Unsigned(meta, IntegerEncoding::U32U16(data))
            }
            // encode as u32 values
            (_, _) => {
                let data = fixed::Fixed::<u32>::from(arr);
                let meta = MetaData::<u64> {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as u64, max as u64)),
                };
                Column::Unsigned(meta, IntegerEncoding::U32U32(data))
            }
        }
    }
}

/// Converts a slice of u16 values into the most compact fixed-width physical
/// encoding.
impl From<&[u16]> for Column {
    fn from(arr: &[u16]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data type.
        match (min, max) {
            // encode as u8 values
            (min, max) if max <= u8::MAX as u16 => {
                let data = fixed::Fixed::<u8>::from(arr);
                let meta = MetaData::<u64> {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as u64, max as u64)),
                };
                Column::Unsigned(meta, IntegerEncoding::U16U8(data))
            }
            // encode as u16 values
            (_, _) => {
                let data = fixed::Fixed::<u16>::from(arr);
                let meta = MetaData::<u64> {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as u64, max as u64)),
                };
                Column::Unsigned(meta, IntegerEncoding::U16U16(data))
            }
        }
    }
}

/// Converts a slice of u8 values into the most compact fixed-width physical
/// encoding.
impl From<&[u8]> for Column {
    fn from(arr: &[u8]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data type.
        let data = fixed::Fixed::<u8>::from(arr);
        let meta = MetaData::<u64> {
            size: data.size(),
            rows: data.num_rows(),
            range: Some((min as u64, max as u64)),
        };
        Column::Unsigned(meta, IntegerEncoding::U8U8(data))
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

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data type.
        match (min, max) {
            // encode as u8 values
            (min, max) if min >= 0 && max <= u8::MAX as i64 => {
                let data = fixed::Fixed::<u8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::I64U8(data))
            }
            // encode as i8 values
            (min, max) if min >= i8::MIN as i64 && max <= i8::MAX as i64 => {
                let data = fixed::Fixed::<i8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::I64I8(data))
            }
            // encode as u16 values
            (min, max) if min >= 0 && max <= u16::MAX as i64 => {
                let data = fixed::Fixed::<u16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::I64U16(data))
            }
            // encode as i16 values
            (min, max) if min >= i16::MIN as i64 && max <= i16::MAX as i64 => {
                let data = fixed::Fixed::<i16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::I64I16(data))
            }
            // encode as u32 values
            (min, max) if min >= 0 && max <= u32::MAX as i64 => {
                let data = fixed::Fixed::<u32>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::I64U32(data))
            }
            // encode as i32 values
            (min, max) if min >= i32::MIN as i64 && max <= i32::MAX as i64 => {
                let data = fixed::Fixed::<i32>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::I64I32(data))
            }
            // otherwise, encode with the same physical type (i64)
            (_, _) => {
                let data = fixed::Fixed::<i64>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::I64I64(data))
            }
        }
    }
}

/// Converts a slice of i32 values into the most compact fixed-width physical
/// encoding.
impl From<&[i32]> for Column {
    fn from(arr: &[i32]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data type.
        match (min, max) {
            // encode as u8 values
            (min, max) if min >= 0 && max <= u8::MAX as i32 => {
                let data = fixed::Fixed::<u8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I32U8(data))
            }
            // encode as i8 values
            (min, max) if min >= i8::MIN as i32 && max <= i8::MAX as i32 => {
                let data = fixed::Fixed::<i8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I32I8(data))
            }
            // encode as u16 values
            (min, max) if min >= 0 && max <= u16::MAX as i32 => {
                let data = fixed::Fixed::<u16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I32U16(data))
            }
            // encode as i16 values
            (min, max) if min >= i16::MIN as i32 && max <= i16::MAX as i32 => {
                let data = fixed::Fixed::<i16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I32I16(data))
            }
            // otherwise, encode with the same physical type (i32)
            (_, _) => {
                let data = fixed::Fixed::<i32>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I32I32(data))
            }
        }
    }
}

/// Converts a slice of i16 values into the most compact fixed-width physical
/// encoding.
impl From<&[i16]> for Column {
    fn from(arr: &[i16]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        // This match is carefully ordered. It prioritises smaller physical
        // datatypes that can safely represent the provided logical data type.
        match (min, max) {
            // encode as i8 values
            (min, max) if min >= i8::MIN as i16 && max <= i8::MAX as i16 => {
                let data = fixed::Fixed::<i8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I16I8(data))
            }
            // encode as u8 values
            (min, max) if min >= 0 && max <= u8::MAX as i16 => {
                let data = fixed::Fixed::<u8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I16U8(data))
            }
            // otherwise, encode with the same physical type (i16)
            (_, _) => {
                let data = fixed::Fixed::<i16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min as i64, max as i64)),
                };
                Column::Integer(meta, IntegerEncoding::I16I16(data))
            }
        }
    }
}

/// Converts a slice of i8 values
impl From<&[i8]> for Column {
    fn from(arr: &[i8]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        let data = fixed::Fixed::<i8>::from(arr);
        let meta = MetaData {
            size: data.size(),
            rows: data.num_rows(),
            range: Some((min as i64, max as i64)),
        };
        Column::Integer(meta, IntegerEncoding::I8I8(data))
    }
}

impl From<arrow::array::Int64Array> for Column {
    fn from(arr: arrow::array::Int64Array) -> Self {
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

        let data = fixed_null::FixedNull::<arrow::datatypes::Int64Type>::from(arr);
        let meta = MetaData {
            size: data.size(),
            rows: data.num_rows(),
            range,
        };
        Column::Integer(meta, IntegerEncoding::I64I64N(data))
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

        let data = fixed::Fixed::<f64>::from(arr);
        let meta = MetaData {
            size: data.size(),
            rows: data.num_rows(),
            range: Some((min, max)),
        };

        Column::Float(meta, FloatEncoding::Fixed64(data))
    }
}

/// Converts a slice of `f32` values into a fixed-width column encoding.
impl From<&[f32]> for Column {
    fn from(arr: &[f32]) -> Self {
        // determine min and max values.
        let mut min = arr[0];
        let mut max = arr[0];
        for &v in arr.iter().skip(1) {
            min = min.min(v);
            max = max.max(v);
        }

        let data = fixed::Fixed::<f32>::from(arr);
        let meta = MetaData {
            size: data.size(),
            rows: data.num_rows(),
            range: Some((min as f64, max as f64)),
        };

        Column::Float(meta, FloatEncoding::Fixed32(data))
    }
}

/// These variants describe supported aggregates that can applied to columnar
/// data.
pub enum AggregateType {
    Count,
    First,
    Last,
    Min,
    Max,
    Sum,
    // TODO - support:
    // Distinct - (edd): not sure this counts as an aggregations. Seems more like a special filter.
    // CountDistinct
    // Percentile
}

/// These variants hold aggregates, which are the results of applying aggregates
/// to column data.
pub enum AggregateResult<'a> {
    // Any type of column can have rows counted. NULL values do not contribute
    // to the count. If all rows are NULL then count will be `0`.
    Count(u64),

    // Only numerical columns with scalar values can be summed. NULL values do
    // not contribute to the sum, but if all rows are NULL then the sum is
    // itself NULL (represented by `None`).
    //
    // TODO(edd): I might explicitly add a Null variant to the Scalar enum like
    // we have with Value...
    Sum(Option<Scalar>),

    // The minimum value in the column data.
    Min(Value<'a>),

    // The maximum value in the column data.
    Max(Value<'a>),

    // The first value in the column data and the corresponding timestamp.
    First(Option<(i64, Value<'a>)>),

    // The last value in the column data and the corresponding timestamp.
    Last(Option<(i64, Value<'a>)>),
}

/// A scalar is a numerical value that can be aggregated.
#[derive(Debug, PartialEq)]

pub enum Scalar {
    I64(i64),
    I32(i32),
    I16(i16),
    I8(i8),

    U64(u64),
    U32(u32),
    U16(u16),
    U8(u8),

    F64(f64),
    F32(f32),
}

macro_rules! typed_scalar_converters {
    ($(($name:ident, $try_name:ident, $type:ident),)*) => {
        $(
            fn $name(&self) -> $type {
                match &self {
                    Self::I64(v) => $type::try_from(*v).unwrap(),
                    Self::I32(v) => $type::try_from(*v).unwrap(),
                    Self::I16(v) => $type::try_from(*v).unwrap(),
                    Self::I8(v) => $type::try_from(*v).unwrap(),
                    Self::U64(v) => $type::try_from(*v).unwrap(),
                    Self::U32(v) => $type::try_from(*v).unwrap(),
                    Self::U16(v) => $type::try_from(*v).unwrap(),
                    Self::U8(v) => $type::try_from(*v).unwrap(),
                    Self::F64(v) => panic!("cannot convert Self::F64"),
                    Self::F32(v) => panic!("cannot convert Scalar::F32"),
                }
            }

            fn $try_name(&self) -> Option<$type> {
                match &self {
                    Self::I64(v) => $type::try_from(*v).ok(),
                    Self::I32(v) => $type::try_from(*v).ok(),
                    Self::I16(v) => $type::try_from(*v).ok(),
                    Self::I8(v) => $type::try_from(*v).ok(),
                    Self::U64(v) => $type::try_from(*v).ok(),
                    Self::U32(v) => $type::try_from(*v).ok(),
                    Self::U16(v) => $type::try_from(*v).ok(),
                    Self::U8(v) => $type::try_from(*v).ok(),
                    Self::F64(v) => panic!("cannot convert Self::F64"),
                    Self::F32(v) => panic!("cannot convert Scalar::F32"),
                }
            }
        )*
    };
}

impl Scalar {
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

    fn as_f32(&self) -> f32 {
        if let Scalar::F32(v) = &self {
            return *v;
        }
        panic!("cannot convert Self to f32");
    }

    fn try_as_f32(&self) -> Option<f32> {
        if let Scalar::F32(v) = &self {
            return Some(*v);
        }
        None
    }

    fn as_f64(&self) -> f64 {
        match &self {
            Scalar::F64(v) => *v,
            Scalar::F32(v) => f64::from(*v),
            _ => unimplemented!("converting integer Scalar to f64 unsupported"),
        }
    }

    fn try_as_f64(&self) -> Option<f64> {
        match &self {
            Scalar::F64(v) => Some(*v),
            Scalar::F32(v) => Some(f64::from(*v)),
            _ => unimplemented!("converting integer Scalar to f64 unsupported"),
        }
    }
}

/// Each variant is a possible value type that can be returned from a column.
#[derive(Debug, PartialEq)]
pub enum Value<'a> {
    // Represents a NULL value in a column row.
    Null,

    // A UTF-8 valid string.
    String(&'a String),

    // An arbitrary byte array.
    ByteArray(&'a [u8]),

    // A boolean value.
    Boolean(bool),

    // A numeric scalar value.
    Scalar(Scalar),
}

impl Value<'_> {
    fn scalar(&self) -> &Scalar {
        if let Self::Scalar(s) = self {
            return s;
        }
        panic!("cannot unwrap Value to Scalar");
    }

    fn string(&self) -> &String {
        if let Self::String(s) = self {
            return s;
        }
        panic!("cannot unwrap Value to String");
    }
}

/// Each variant is a typed vector of materialised values for a column. NULL
/// values are represented as None
#[derive(Debug, PartialEq)]
pub enum Values {
    // UTF-8 valid unicode strings
    String(arrow::array::StringArray),

    F64(arrow::array::Float64Array),
    F32(arrow::array::Float32Array),

    I64(arrow::array::Int64Array),
    I32(arrow::array::Int32Array),
    I16(arrow::array::Int16Array),
    I8(arrow::array::Int8Array),

    U64(arrow::array::UInt64Array),
    U32(arrow::array::UInt32Array),
    U16(arrow::array::UInt16Array),
    U8(arrow::array::UInt8Array),

    // Boolean values
    Bool(arrow::array::BooleanArray),

    // Arbitrary byte arrays
    ByteArray(arrow::array::UInt8Array),
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
    None,
    Some(RowIDs),

    // All allows us to indicate to the caller that all possible rows are
    // represented, without having to create a container to store all those ids.
    All,
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
#[derive(PartialEq, Debug)]
pub enum RowIDs {
    Bitmap(Bitmap),
    Vector(Vec<u32>),
}

impl RowIDs {
    pub fn new_bitmap() -> Self {
        Self::Bitmap(Bitmap::create())
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
            RowIDs::Bitmap(bm) => bm.to_vec(),
            RowIDs::Vector(arr) => arr.clone(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            RowIDs::Bitmap(ids) => ids.cardinality() as usize,
            RowIDs::Vector(ids) => ids.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            RowIDs::Bitmap(ids) => ids.is_empty(),
            RowIDs::Vector(ids) => ids.is_empty(),
        }
    }

    pub fn clear(&mut self) {
        match self {
            RowIDs::Bitmap(ids) => ids.clear(),
            RowIDs::Vector(ids) => ids.clear(),
        }
    }

    pub fn add_range(&mut self, from: u32, to: u32) {
        match self {
            RowIDs::Bitmap(ids) => ids.add_range(from as u64..to as u64),
            RowIDs::Vector(ids) => ids.extend(from..to),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use arrow_deps::arrow::array::{
        Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, StringArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };

    #[test]
    fn from_arrow_string_array() {
        let input = vec![None, Some("world"), None, Some("hello")];
        let arr = StringArray::from(input);

        let col = Column::from(arr);
        if let Column::String(meta, StringEncoding::RLE(mut enc)) = col {
            assert_eq!(
                meta,
                super::MetaData::<String> {
                    size: 0,
                    rows: 4,
                    range: Some(("hello".to_string(), "world".to_string())),
                }
            );

            assert_eq!(
                enc.all_values(vec![]),
                vec![
                    None,
                    Some(&"world".to_string()),
                    None,
                    Some(&"hello".to_string())
                ]
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
        if let Column::String(meta, StringEncoding::RLE(mut enc)) = col {
            assert_eq!(
                meta,
                super::MetaData::<String> {
                    size: 0,
                    rows: 2,
                    range: Some(("hello".to_string(), "world".to_string())),
                }
            );

            assert_eq!(
                enc.all_values(vec![]),
                vec![Some(&"world".to_string()), Some(&"hello".to_string())]
            );

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
    fn from_i32_slice() {
        let input = &[-1, i8::MAX as i32];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I32I8(_))
        ));

        let input = &[0, u8::MAX as i32];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I32U8(_))
        ));

        let input = &[-1, i16::MAX as i32];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I32I16(_))
        ));

        let input = &[0, u16::MAX as i32];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I32U16(_))
        ));

        let input = &[-1, i32::MAX];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I32I32(_))
        ));

        // validate min/max check
        let input = &[0, -12, u8::MAX as i32, 5];
        let col = Column::from(&input[..]);
        if let Column::Integer(meta, IntegerEncoding::I32I16(_)) = col {
            assert_eq!(meta.size, 32); // 4 i16s (8b) and a vec (24b)
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((-12, u8::MAX as i64)));
        } else {
            panic!("invalid variant");
        }
    }

    #[test]
    fn from_i16_slice() {
        let input = &[-1, i8::MAX as i16];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I16I8(_))
        ));

        let input = &[0, u8::MAX as i16];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I16U8(_))
        ));

        let input = &[-1, i16::MAX as i16];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I16I16(_))
        ));

        // validate min/max check
        let input = &[0, -12, u8::MAX as i16, 5];
        let col = Column::from(&input[..]);
        if let Column::Integer(meta, IntegerEncoding::I16I16(_)) = col {
            assert_eq!(meta.size, 32); // 4 i16s (8b) and a vec (24b)
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((-12, u8::MAX as i64)));
        } else {
            panic!("invalid variant");
        }
    }

    #[test]
    fn from_i8_slice() {
        let input = &[-1, i8::MAX];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I8I8(_))
        ));

        // validate min/max check
        let input = &[0, -12, i8::MAX, 5];
        let col = Column::from(&input[..]);
        if let Column::Integer(meta, IntegerEncoding::I8I8(_)) = col {
            assert_eq!(meta.size, 28); // 4 i8s (4b) and a vec (24b)
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((-12, i8::MAX as i64)));
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
    fn from_u32_slice() {
        let input = &[0, u8::MAX as u32];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U32U8(_))
        ));

        let input = &[0, u16::MAX as u32];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U32U16(_))
        ));

        let input = &[0, u32::MAX as u32];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U32U32(_))
        ));

        // validate min/max check
        let input = &[13, 12, u16::MAX as u32, 5];
        let col = Column::from(&input[..]);
        if let Column::Unsigned(meta, IntegerEncoding::U32U16(_)) = col {
            assert_eq!(meta.size, 32); // 4 u16s (8b) and a vec (24b)
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((5, u16::MAX as u64)));
        } else {
            panic!("invalid variant");
        }
    }

    #[test]
    fn from_u16_slice() {
        let input = &[0, u8::MAX as u16];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U16U8(_))
        ));

        let input = &[0, u16::MAX as u16];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U16U16(_))
        ));

        // validate min/max check
        let input = &[13, 12, u8::MAX as u16, 5];
        let col = Column::from(&input[..]);
        if let Column::Unsigned(meta, IntegerEncoding::U16U8(_)) = col {
            assert_eq!(meta.size, 28); // 4 u8s (4b) and a vec (24b)
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((5, u8::MAX as u64)));
        } else {
            panic!("invalid variant");
        }
    }

    #[test]
    fn from_u8_slice() {
        let input = &[0, u8::MAX];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Unsigned(_, IntegerEncoding::U8U8(_))
        ));

        // validate min/max check
        let input = &[13, 12, u8::MAX, 5];
        let col = Column::from(&input[..]);
        if let Column::Unsigned(meta, IntegerEncoding::U8U8(_)) = col {
            assert_eq!(meta.size, 28); // 4 u8s (4b) and a vec (24b)
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((5, u8::MAX as u64)));
        } else {
            panic!("invalid variant");
        }
    }

    #[test]
    fn value() {
        // The Scalar variant always represents the logical type of the column.
        // The `value` method always returns values according to the logical
        // type, no matter what the underlying physical type might be.

        // physical type of `col` will be `i16` but logical type is `i64`
        let col = Column::from(&[0_i64, 1, 200, 20, -1][..]);
        assert_eq!(col.value(4), Value::Scalar(Scalar::I64(-1)));

        // physical type of `col` will be `u16` but logical type is `u64`
        let col = Column::from(&[20_u64, 300][..]);
        assert_eq!(col.value(1), Value::Scalar(Scalar::U64(300)));

        // physical type of `col` will be `u8` but logical type is `u32`
        let col = Column::from(&[20_u32, 3][..]);
        assert_eq!(col.value(0), Value::Scalar(Scalar::U32(20)));

        // physical type of `col` will be `u8` but logical type is `u16`
        let col = Column::from(&[20_u16, 3][..]);
        assert_eq!(col.value(1), Value::Scalar(Scalar::U16(3)));

        // physical and logical type of `col` will be `u8`
        let col = Column::from(&[243_u8, 198][..]);
        assert_eq!(col.value(0), Value::Scalar(Scalar::U8(243)));

        let col = Column::from(&[-19.2, -30.2][..]);
        assert_eq!(col.value(0), Value::Scalar(Scalar::F64(-19.2)));

        let col = Column::from(&[Some("a"), Some("b"), None, Some("c")][..]);
        assert_eq!(col.value(1), Value::String(&"b".to_owned()));
        assert_eq!(col.value(2), Value::Null);
    }

    #[test]
    fn values() {
        // physical type of `col` will be `i16` but logical type is `i64`
        let col = Column::from(&[0_i64, 1, 200, 20, -1][..]);
        assert_eq!(
            col.values(&[0, 2, 3]),
            Values::I64(Int64Array::from(vec![0, 200, 20]))
        );

        // physical type of `col` will be `i16` but logical type is `i32`
        let col = Column::from(&[0_i32, 1, 200, 20, -1][..]);
        assert_eq!(
            col.values(&[0, 2, 3]),
            Values::I32(Int32Array::from(vec![0, 200, 20]))
        );

        // physical and logical type of `col` will be `i16`
        let col = Column::from(&[0_i16, 1, 200, 20, -1][..]);
        assert_eq!(
            col.values(&[0, 2, 3]),
            Values::I16(Int16Array::from(vec![0, 200, 20]))
        );

        // physical and logical type of `col` will be `i8`
        let col = Column::from(&[0_i8, 1, 127, 20, -1][..]);
        assert_eq!(
            col.values(&[0, 2, 3]),
            Values::I8(Int8Array::from(vec![0, 127, 20]))
        );

        // physical type of `col` will be `u8` but logical type is `u64`
        let col = Column::from(&[0_u64, 1, 200, 20, 100][..]);
        assert_eq!(
            col.values(&[3, 4]),
            Values::U64(UInt64Array::from(vec![20, 100]))
        );

        // physical type of `col` will be `u8` but logical type is `u32`
        let col = Column::from(&[0_u32, 1, 200, 20, 100][..]);
        assert_eq!(
            col.values(&[3, 4]),
            Values::U32(UInt32Array::from(vec![20, 100]))
        );

        // physical type of `col` will be `u8` but logical type is `u16`
        let col = Column::from(&[0_u16, 1, 200, 20, 100][..]);
        assert_eq!(
            col.values(&[3, 4]),
            Values::U16(UInt16Array::from(vec![20, 100]))
        );

        // physical and logical type of `col` will be `u8`
        let col = Column::from(&[0_u8, 1, 200, 20, 100][..]);
        assert_eq!(
            col.values(&[3, 4]),
            Values::U8(UInt8Array::from(vec![20, 100]))
        );

        // physical and logical type of `col` will be `f64`
        let col = Column::from(&[0.0, 1.1, 20.2, 22.3, 100.1324][..]);
        assert_eq!(
            col.values(&[1, 3]),
            Values::F64(Float64Array::from(vec![1.1, 22.3]))
        );

        // physical and logical type of `col` will be `f32`
        let col = Column::from(&[0.0_f32, 1.1, 20.2, 22.3, 100.1324][..]);
        assert_eq!(
            col.values(&[1, 3]),
            Values::F32(Float32Array::from(vec![1.1, 22.3]))
        );

        let col = Column::from(&[Some("a"), Some("b"), None, Some("c")][..]);
        assert_eq!(
            col.values(&[1, 2, 3]),
            Values::String(StringArray::from(vec![Some("b"), None, Some("c")]))
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

        let col = Column::from(&input[..]);
        let mut row_ids =
            col.row_ids_filter(cmp::Operator::Equal, Value::String(&"Badlands".to_string()));
        assert_eq!(row_ids.unwrap().to_vec(), vec![0]);

        row_ids = col.row_ids_filter(cmp::Operator::Equal, Value::String(&"Factory".to_string()));
        assert!(matches!(row_ids, RowIDsOption::None));

        row_ids = col.row_ids_filter(
            cmp::Operator::GT,
            Value::String(&"Adam Raised a Cain".to_string()),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 2, 3, 6]);

        row_ids = col.row_ids_filter(
            cmp::Operator::LTE,
            Value::String(&"Streets of Fire".to_string()),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 2, 3, 6]);

        row_ids = col.row_ids_filter(
            cmp::Operator::LT,
            Value::String(&"Something in the Night".to_string()),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 2, 6]);

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
            cmp::Operator::NotEqual,
            Value::String(&"Adam Raised a Cain".to_string()),
        );
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter(
            cmp::Operator::GT,
            Value::String(&"Adam Raised a Cain".to_string()),
        );
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter(
            cmp::Operator::NotEqual,
            Value::String(&"Thunder Road".to_string()),
        );
        assert!(matches!(row_ids, RowIDsOption::All));
    }

    #[test]
    fn row_ids_filter_int() {
        let input = &[100, 200, 300, 2, 200, 22, 30];

        let col = Column::from(&input[..]);
        let mut row_ids = col.row_ids_filter(cmp::Operator::Equal, Value::Scalar(Scalar::I32(200)));
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 4]);

        row_ids = col.row_ids_filter(cmp::Operator::Equal, Value::Scalar(Scalar::I32(2000)));
        assert!(matches!(row_ids, RowIDsOption::None));

        row_ids = col.row_ids_filter(cmp::Operator::GT, Value::Scalar(Scalar::I32(2)));
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 4, 5, 6]);

        row_ids = col.row_ids_filter(cmp::Operator::GTE, Value::Scalar(Scalar::I32(2)));
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter(cmp::Operator::NotEqual, Value::Scalar(Scalar::I32(-1257)));
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter(cmp::Operator::LT, Value::Scalar(Scalar::I64(i64::MAX)));
        assert!(matches!(row_ids, RowIDsOption::All));

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
        row_ids = col.row_ids_filter(cmp::Operator::GT, Value::Scalar(Scalar::I64(10)));
        println!("{:?}", row_ids);
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 4, 5, 6]);
    }

    #[test]
    fn row_ids_filter_uint() {
        let input = &[100_u32, 200, 300, 2, 200, 22, 30];

        let col = Column::from(&input[..]);
        let mut row_ids = col.row_ids_filter(cmp::Operator::Equal, Value::Scalar(Scalar::I32(200)));
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 4]);

        row_ids = col.row_ids_filter(cmp::Operator::Equal, Value::Scalar(Scalar::U16(2000)));
        assert!(matches!(row_ids, RowIDsOption::None));

        row_ids = col.row_ids_filter(cmp::Operator::GT, Value::Scalar(Scalar::U32(2)));
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 4, 5, 6]);

        row_ids = col.row_ids_filter(cmp::Operator::GTE, Value::Scalar(Scalar::U64(2)));
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter(cmp::Operator::NotEqual, Value::Scalar(Scalar::I32(-1257)));
        assert!(matches!(row_ids, RowIDsOption::All));
    }

    #[test]
    fn row_ids_filter_float() {
        let input = &[100.2, 200.0, 300.1, 2.22, -200.2, 22.2, 30.2];

        let col = Column::from(&input[..]);
        let mut row_ids =
            col.row_ids_filter(cmp::Operator::Equal, Value::Scalar(Scalar::F32(200.0)));
        assert_eq!(row_ids.unwrap().to_vec(), vec![1]);

        row_ids = col.row_ids_filter(cmp::Operator::Equal, Value::Scalar(Scalar::F64(2000.0)));
        assert!(matches!(row_ids, RowIDsOption::None));

        row_ids = col.row_ids_filter(cmp::Operator::GT, Value::Scalar(Scalar::F64(-200.0)));
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 3, 5, 6]);

        row_ids = col.row_ids_filter(cmp::Operator::GTE, Value::Scalar(Scalar::F64(-200.2)));
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter(
            cmp::Operator::NotEqual,
            Value::Scalar(Scalar::F32(-1257.029)),
        );
        assert!(matches!(row_ids, RowIDsOption::All));
    }

    #[test]
    fn row_ids_range() {
        let input = &[100, 200, 300, 2, 200, 22, 30];

        let col = Column::from(&input[..]);
        let mut row_ids = col.row_ids_filter_range(
            (cmp::Operator::GT, Value::Scalar(Scalar::I32(100))),
            (cmp::Operator::LT, Value::Scalar(Scalar::I32(300))),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 4]);

        row_ids = col.row_ids_filter_range(
            (cmp::Operator::GTE, Value::Scalar(Scalar::I32(200))),
            (cmp::Operator::LTE, Value::Scalar(Scalar::I32(300))),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![1, 2, 4]);

        row_ids = col.row_ids_filter_range(
            (cmp::Operator::GTE, Value::Scalar(Scalar::I32(23333))),
            (cmp::Operator::LTE, Value::Scalar(Scalar::I32(999999))),
        );
        assert!(matches!(row_ids, RowIDsOption::None));

        row_ids = col.row_ids_filter_range(
            (cmp::Operator::GT, Value::Scalar(Scalar::I32(-100))),
            (cmp::Operator::LT, Value::Scalar(Scalar::I32(301))),
        );
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter_range(
            (cmp::Operator::GTE, Value::Scalar(Scalar::I32(2))),
            (cmp::Operator::LTE, Value::Scalar(Scalar::I32(300))),
        );
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter_range(
            (cmp::Operator::GTE, Value::Scalar(Scalar::I32(87))),
            (cmp::Operator::LTE, Value::Scalar(Scalar::I32(999999))),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 4]);

        row_ids = col.row_ids_filter_range(
            (cmp::Operator::GTE, Value::Scalar(Scalar::I32(0))),
            (
                cmp::Operator::NotEqual,
                Value::Scalar(Scalar::I64(i64::MAX)),
            ),
        );
        assert!(matches!(row_ids, RowIDsOption::All));

        row_ids = col.row_ids_filter_range(
            (cmp::Operator::GTE, Value::Scalar(Scalar::I32(0))),
            (cmp::Operator::NotEqual, Value::Scalar(Scalar::I64(99))),
        );
        assert_eq!(row_ids.unwrap().to_vec(), vec![0, 1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn might_contain_value() {
        let input = &[100i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);

        let cases: Vec<(Scalar, bool)> = vec![
            (Scalar::U64(200), true),
            (Scalar::U32(200), true),
            (Scalar::U8(200), true),
            (Scalar::I64(100), true),
            (Scalar::I32(30), true),
            (Scalar::I8(2), true),
            (Scalar::U64(100000000), false),
            (Scalar::I64(-1), false),
            (Scalar::U64(u64::MAX), false),
        ];

        for (scalar, result) in cases {
            assert_eq!(col.might_contain_value(&Value::Scalar(scalar)), result);
        }

        let input = &[100i16, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);

        let cases: Vec<(Scalar, bool)> = vec![
            (Scalar::U64(200), true),
            (Scalar::U16(200), true),
            (Scalar::U8(200), true),
            (Scalar::I64(100), true),
            (Scalar::I32(30), true),
            (Scalar::I8(2), true),
            (Scalar::U64(100000000), false),
            (Scalar::I64(-1), false),
            (Scalar::U64(u64::MAX), false),
        ];

        for (scalar, result) in cases {
            assert_eq!(col.might_contain_value(&Value::Scalar(scalar)), result);
        }

        let input = &[100u64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);

        let cases: Vec<(Scalar, bool)> = vec![
            (Scalar::U64(200), true),
            (Scalar::U32(200), true),
            (Scalar::U8(200), true),
            (Scalar::I64(100), true),
            (Scalar::I32(30), true),
            (Scalar::I8(2), true),
            (Scalar::U64(100000000), false),
            (Scalar::I64(-1), false),
        ];

        for (scalar, result) in cases {
            assert_eq!(col.might_contain_value(&Value::Scalar(scalar)), result);
        }

        let input = &[100.0, 200.2, 300.2];
        let col = Column::from(&input[..]);

        let cases: Vec<(Scalar, bool)> =
            vec![(Scalar::F64(100.0), true), (Scalar::F32(100.0), true)];

        for (scalar, result) in cases {
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
            (cmp::Operator::GT, Scalar::I8(-99), true),
            (cmp::Operator::GT, Scalar::I64(100), false),
            (cmp::Operator::LT, Scalar::I64(300), false),
            (cmp::Operator::LTE, Scalar::I32(300), true),
            (cmp::Operator::Equal, Scalar::I32(2), false),
            (cmp::Operator::NotEqual, Scalar::I32(2), false),
            (cmp::Operator::NotEqual, Scalar::I64(1), true),
            (cmp::Operator::NotEqual, Scalar::I64(301), true),
        ];

        for (op, scalar, result) in cases {
            assert_eq!(
                col.predicate_matches_all_values(&op, &Value::Scalar(scalar)),
                result
            );
        }

        // Future improvement would be to support this type of check.
        let input = &[100i8, -20];
        let col = Column::from(&input[..]);
        assert_eq!(
            col.predicate_matches_all_values(
                &cmp::Operator::LT,
                &Value::Scalar(Scalar::U64(u64::MAX))
            ),
            false
        );
    }

    #[test]
    fn evaluate_predicate_on_meta() {
        let input = &[100i64, 200, 300, 2, 200, 22, 30];
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
            (cmp::Operator::GT, Scalar::I8(-99), PredicateMatch::All),
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
            (cmp::Operator::LTE, Scalar::I32(300), PredicateMatch::All),
            (
                cmp::Operator::Equal,
                Scalar::I32(2),
                PredicateMatch::SomeMaybe,
            ),
            (
                cmp::Operator::NotEqual,
                Scalar::I32(2),
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
            (cmp::Operator::LTE, Scalar::I8(-100), PredicateMatch::None),
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
        let input = &[100i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.min(&[0, 1, 3][..]), Value::Scalar(Scalar::I64(2)));
        assert_eq!(col.min(&[0, 1, 2][..]), Value::Scalar(Scalar::I64(100)));

        let input = &[100u8, 200, 245, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.min(&[4, 6][..]), Value::Scalar(Scalar::U8(30)));

        let input = &[Some("hello"), None, Some("world")];
        let col = Column::from(&input[..]);
        assert_eq!(col.min(&[0, 1, 2][..]), Value::String(&"hello".to_string()));
        assert_eq!(col.min(&[1][..]), Value::Null);
    }

    #[test]
    fn max() {
        let input = &[100i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.max(&[0, 1, 3][..]), Value::Scalar(Scalar::I64(200)));
        assert_eq!(col.max(&[0, 1, 2][..]), Value::Scalar(Scalar::I64(300)));

        let input = &[10.2_f32, -2.43, 200.2];
        let col = Column::from(&input[..]);
        assert_eq!(col.max(&[0, 1, 2][..]), Value::Scalar(Scalar::F32(200.2)));

        let input = vec![None, Some(200), None];
        let arr = Int64Array::from(input);
        let col = Column::from(arr);
        assert_eq!(col.max(&[0, 1, 2][..]), Value::Scalar(Scalar::I64(200)));

        let input = &[Some("hello"), None, Some("world")];
        let col = Column::from(&input[..]);
        assert_eq!(col.max(&[0, 1, 2][..]), Value::String(&"world".to_string()));
        assert_eq!(col.max(&[1][..]), Value::Null);
    }

    #[test]
    fn sum() {
        let input = &[100i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.sum(&[0, 1, 3][..]), Value::Scalar(Scalar::I64(302)));
        assert_eq!(col.sum(&[0, 1, 2][..]), Value::Scalar(Scalar::I64(600)));

        let input = &[10.2_f32, -2.43, 200.2];
        let col = Column::from(&input[..]);
        assert_eq!(col.sum(&[0, 1, 2][..]), Value::Scalar(Scalar::F32(207.97)));

        let input = vec![None, Some(200), None];
        let arr = Int64Array::from(input);
        let col = Column::from(arr);
        assert_eq!(col.sum(&[0, 1, 2][..]), Value::Scalar(Scalar::I64(200)));
    }

    #[test]
    fn count() {
        let input = &[100i64, 200, 300, 2, 200, 22, 30];
        let col = Column::from(&input[..]);
        assert_eq!(col.count(&[0, 1, 3][..]), 3);

        let input = &[10.2_f32, -2.43, 200.2];
        let col = Column::from(&input[..]);
        assert_eq!(col.count(&[0, 1][..]), 2);

        let input = vec![None, Some(200), None];
        let arr = Int64Array::from(input);
        let col = Column::from(arr);
        assert_eq!(col.count(&[0, 1, 2][..]), 1);
        assert_eq!(col.count(&[0, 2][..]), 0);
    }
}
