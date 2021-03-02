use std::{collections::BTreeSet, convert::TryFrom};
use std::{mem::size_of, sync::Arc};

use arrow_deps::arrow;

use crate::AggregateType;

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
            pub fn $name(&self) -> $type {
                match &self {
                    Self::I64(v) => $type::try_from(*v).unwrap(),
                    Self::U64(v) => $type::try_from(*v).unwrap(),
                    Self::F64(_) => panic!("cannot convert Self::F64"),
                    Self::Null => panic!("cannot convert Scalar::Null"),
                }
            }

            pub fn $try_name(&self) -> Option<$type> {
                match &self {
                    Self::I64(v) => $type::try_from(*v).ok(),
                    Self::U64(v) => $type::try_from(*v).ok(),
                    Self::F64(_) => panic!("cannot convert Self::F64"),
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

    pub fn as_f64(&self) -> f64 {
        match &self {
            Self::F64(v) => *v,
            _ => unimplemented!("converting integer Scalar to f64 unsupported"),
        }
    }

    pub fn try_as_f64(&self) -> Option<f64> {
        match &self {
            Self::F64(v) => Some(*v),
            _ => unimplemented!("converting integer Scalar to f64 unsupported"),
        }
    }
}

impl std::ops::AddAssign<&Scalar> for Scalar {
    fn add_assign(&mut self, rhs: &Self) {
        if rhs.is_null() {
            // Adding NULL does nothing.
            return;
        }

        match self {
            Self::F64(v) => {
                if let Self::F64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            Self::I64(v) => {
                if let Self::I64(other) = rhs {
                    *v += *other;
                } else {
                    panic!("invalid AddAssign types");
                };
            }
            Self::U64(v) => {
                if let Self::U64(other) = rhs {
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
            Self::Null => write!(f, "NULL"),
            Self::I64(v) => write!(f, "{}", v),
            Self::U64(v) => write!(f, "{}", v),
            Self::F64(v) => write!(f, "{}", v),
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

impl OwnedValue {
    /// The size in bytes of this value.
    pub fn size(&self) -> usize {
        let self_size = size_of::<Self>();
        match self {
            Self::String(s) => s.len() + self_size,
            Self::ByteArray(arr) => arr.len() + self_size,
            _ => self_size,
        }
    }
}

impl PartialEq<Value<'_>> for OwnedValue {
    fn eq(&self, other: &Value<'_>) -> bool {
        match (&self, other) {
            (Self::String(a), Value::String(b)) => a == b,
            (Self::Scalar(a), Value::Scalar(b)) => a == b,
            (Self::Boolean(a), Value::Boolean(b)) => a == b,
            (Self::ByteArray(a), Value::ByteArray(b)) => a == b,
            _ => false,
        }
    }
}

impl PartialOrd<Value<'_>> for OwnedValue {
    fn partial_cmp(&self, other: &Value<'_>) -> Option<std::cmp::Ordering> {
        match (&self, other) {
            (Self::String(a), Value::String(b)) => Some(a.as_str().cmp(b)),
            (Self::Scalar(a), Value::Scalar(b)) => a.partial_cmp(b),
            (Self::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            (Self::ByteArray(a), Value::ByteArray(b)) => a.as_slice().partial_cmp(*b),
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
impl From<Values<'_>> for arrow::array::ArrayRef {
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
            Self::I64(_) => panic!("cannot take Vec<u32> out of I64 variant"),
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn size() {
        let v1 = OwnedValue::Null;
        assert_eq!(v1.size(), 32);

        let v1 = OwnedValue::Scalar(Scalar::I64(22));
        assert_eq!(v1.size(), 32);

        let v1 = OwnedValue::String("11morebytes".to_owned());
        assert_eq!(v1.size(), 43);

        let v1 = OwnedValue::ByteArray(vec![2, 44, 252]);
        assert_eq!(v1.size(), 35);
    }
}
