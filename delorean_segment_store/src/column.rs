/// These variants describe supported aggregates that can applied to columnar
/// data.
pub enum AggregateType {
    Count,
    First,
    Last,
    Min,
    Max,
    Sum,
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

    // A numeric scalar value.
    Scalar(Scalar),
}

/// Each variant is a typed vector of materialised values for a column. NULL
/// values are represented as None
pub enum Values<'a> {
    // UTF-8 valid unicode strings
    String(Vec<&'a Option<std::string::String>>),

    // 64-bit floating point values
    Float(Vec<Option<f64>>),

    // 64-bit signed integer values
    Integer(Vec<Option<i64>>),

    // 64-bit unsigned integer values
    Unsigned(Vec<Option<u64>>),

    // Boolean values
    Bool(Vec<Option<bool>>),

    // Arbitrary byte arrays
    ByteArray(Vec<Option<&'a [u8]>>),
}
