pub mod cmp;
pub mod dictionary;
pub mod fixed;
pub mod fixed_null;

use std::collections::BTreeSet;

use croaring::Bitmap;

use delorean_arrow::{arrow, arrow::array::Array};

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
    pub fn num_rows(&self) -> usize {
        todo!()
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
        todo!()
    }

    /// All values present at the provided logical row ids.
    pub fn values(&self, row_ids: &[u32]) -> Values {
        todo!()
    }

    // The distinct set of values found at the logical row ids.
    pub fn distinct_values(&self, row_ids: &[u32]) -> ValuesSet<'_> {
        todo!()
    }

    //
    // Methods for getting encoded (compressed) values.
    //

    /// The encoded values found at the provided logical row ids.
    pub fn encoded_values(&self, row_ids: &[u32]) -> Values {
        todo!()
    }

    /// All encoded values in the column.
    pub fn all_encoded_values(&self) -> Values {
        todo!()
    }

    //
    // Methods for filtering
    //

    /// Determine the set of row ids that satisfy the predicate.
    pub fn row_ids_filter(&self, op: cmp::Operator, value: Value<'_>) -> RowIDsOption {
        todo!()
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
        todo!()
    }

    //
    // Methods for selecting
    //

    /// The minimum value present within the set of rows.
    pub fn min(&self, row_ids: &[u32]) -> Values {
        todo!()
    }

    /// The minimum value present within the set of rows.
    pub fn max(&self, row_ids: &[u32]) -> Values {
        todo!()
    }

    //
    // Methods for aggregating
    //

    /// The summation of all non-null values located at the provided rows.
    pub fn sum(&self, row_ids: &[u32]) -> Values {
        todo!()
    }

    /// The count of all non-null values located at the provided rows.
    pub fn count(&self, row_ids: &[u32]) -> Values {
        todo!()
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
pub struct MetaData<T> {
    // The total size of the column in bytes.
    size: u64,

    // The total number of rows in the column.
    rows: u32,

    // The minimum and maximum value for this column.
    range: Option<(T, T)>,
}

pub enum StringEncoding {
    RLE(dictionary::RLE),
    // TODO - simple array encoding, e.g., via Arrow String array.
}

/// This implementation is concerned with how to produce string columns with
/// different encodings.
impl StringEncoding {
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
    // TODO - add all the other possible integer combinations.

    // Nullable encodings
    I64I64N(fixed_null::FixedNull<arrow::datatypes::Int64Type>),
}

pub enum FloatEncoding {
    Fixed64(fixed::Fixed<f64>),
    Fixed32(fixed::Fixed<f32>),
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

/// This method supports converting a slice of signed integers into the most
/// compact fixed-width physical encoding. The chosen physical encoding is
/// encapsulated in the returned `Column` variant.
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
        Column::Float(
            MetaData::default(),
            FloatEncoding::Fixed64(fixed::Fixed::<f64>::from(arr)),
        )
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
pub enum Scalar {
    Float(f64),
    Integer(i64),
    Unsigned(u64),
}

/// Each variant is a possible value type that can be returned from a column.
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

/// Each variant is a typed vector of materialised values for a column. NULL
/// values are represented as None
pub enum Values {
    // UTF-8 valid unicode strings
    String(arrow::array::StringArray),

    // 64-bit floating point values
    Float(arrow::array::Float64Array),

    // 64-bit signed integer values
    Integer(arrow::array::Int64Array),

    // 64-bit unsigned integer values
    Unsigned(arrow::array::UInt64Array),

    // Boolean values
    Bool(arrow::array::BooleanArray),

    // Arbitrary byte arrays
    ByteArray(arrow::array::UInt8Array),
}

pub enum ValuesSet<'a> {
    // UTF-8 valid unicode strings
    String(BTreeSet<Option<&'a String>>),
    Float64(BTreeSet<Option<f64>>),
    Integer64(BTreeSet<Option<i64>>),
    Unsigned64(BTreeSet<Option<u64>>),
    Bool(BTreeSet<Option<bool>>),
    ByteArray(BTreeSet<Option<&'a [u8]>>),
}

/// A specific type of Option for `RowIDs` where the notion of all rows ids is
/// represented.
pub enum RowIDsOption {
    None,
    Some(RowIDs),

    // All allows us to indicate to the caller that all possible rows are
    // represented, without having to create a container to store all those ids.
    All,
}

/// Represents vectors of row IDs, which are usually used for intermediate
/// results as a method of late materialisation.
#[derive(PartialEq, Debug)]
pub enum RowIDs {
    Bitmap(Bitmap),
    Vector(Vec<u32>),
}

impl RowIDs {
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
    use delorean_arrow::arrow::array::StringArray;

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
        let input = &[0, u8::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64U8(_))
        ));

        let input = &[0, u16::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64U16(_))
        ));

        let input = &[0, u32::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64U32(_))
        ));

        let input = &[-1, i8::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64I8(_))
        ));

        let input = &[-1, i16::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64I16(_))
        ));

        let input = &[-1, i32::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::I64I32(_))
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
            assert_eq!(meta.size, 40); // 3 i32s and a vec
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((-12, u16::MAX as i64)));
        } else {
            panic!("invalid variant");
        }
    }
}
