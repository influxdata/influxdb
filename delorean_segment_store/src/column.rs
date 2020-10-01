use delorean_arrow::arrow;

/// The possible logical types that column values can have. All values in a
/// column have the same physical type.
pub enum Column {
    String,    // TODO - UTF-8 string
    Float,     // TODO - 64-bit floating point values
    Integer,   // TODO - 64-bit signed integers
    Unsigned,  // TODO - 64-bit unsigned integers
    Bool,      // TODO - booleans
    ByteArray, // TODO - arbitrary bytes
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
