pub mod cmp;
pub mod dictionary;
pub mod fixed;
pub mod fixed_null;

use croaring::Bitmap;

use delorean_arrow::arrow;

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

#[derive(Default)]
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

pub enum IntegerEncoding {
    S64S64(fixed::Fixed<i64>),
    S64S32(fixed::Fixed<i32>),
    S64U32(fixed::Fixed<u32>),
    S64S16(fixed::Fixed<i16>),
    S64U16(fixed::Fixed<u16>),
    S64S8(fixed::Fixed<i8>),
    S64U8(fixed::Fixed<u8>),
    // TODO - add all the other possible integer combinations.
}

pub enum FloatEncoding {
    Fixed64(fixed::Fixed<f64>),
    Fixed32(fixed::Fixed<f32>),
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

        match (min, max) {
            (min, max) if min >= 0 && max <= u8::MAX as i64 => {
                let data = fixed::Fixed::<u8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::S64U8(data))
            }
            (min, max) if min >= i8::MIN as i64 && max <= i8::MAX as i64 => {
                let data = fixed::Fixed::<i8>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::S64S8(data))
            }
            (min, max) if min >= 0 && max <= u16::MAX as i64 => {
                let data = fixed::Fixed::<u16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::S64U16(data))
            }
            (min, max) if min >= i16::MIN as i64 && max <= i16::MAX as i64 => {
                let data = fixed::Fixed::<i16>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::S64S16(data))
            }
            (min, max) if min >= 0 && max <= u32::MAX as i64 => {
                let data = fixed::Fixed::<u32>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::S64U32(data))
            }
            (min, max) if min >= i32::MIN as i64 && max <= i32::MAX as i64 => {
                let data = fixed::Fixed::<i32>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::S64S32(data))
            }
            (_, _) => {
                let data = fixed::Fixed::<i64>::from(arr);
                let meta = MetaData {
                    size: data.size(),
                    rows: data.num_rows(),
                    range: Some((min, max)),
                };
                Column::Integer(meta, IntegerEncoding::S64S64(data))
            }
        }
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
    String(Vec<arrow::array::StringArray>),

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

    #[test]
    fn from_i64_slice() {
        let input = &[0, u8::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::S64U8(_))
        ));

        let input = &[0, u16::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::S64U16(_))
        ));

        let input = &[0, u32::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::S64U32(_))
        ));

        let input = &[-1, i8::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::S64S8(_))
        ));

        let input = &[-1, i16::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::S64S16(_))
        ));

        let input = &[-1, i32::MAX as i64];
        assert!(matches!(
            Column::from(&input[..]),
            Column::Integer(_, IntegerEncoding::S64S32(_))
        ));

        // validate min/max check
        let input = &[0, -12, u16::MAX as i64, 5];
        let col = Column::from(&input[..]);
        if let Column::Integer(meta, IntegerEncoding::S64S32(_)) = col {
            assert_eq!(meta.size, 40); // 3 i32s and a vec
            assert_eq!(meta.rows, 4);
            assert_eq!(meta.range, Some((-12, u16::MAX as i64)));
        } else {
            panic!("invalid variant");
        }
    }
}
