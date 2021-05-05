use std::collections::BTreeSet;
use std::convert::From;

use super::encoding;

use observability_deps::tracing::debug;

#[derive(Debug)]
pub enum Set<'a> {
    String(BTreeSet<&'a std::string::String>),
    Integer(BTreeSet<i64>),
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Value<'a> {
    Null,
    String(&'a str),
    Scalar(Scalar),
}

pub enum Values<'a> {
    String(Vec<&'a Option<std::string::String>>),
    Float(Vec<Option<f64>>),
    Integer(Vec<Option<i64>>),
}

impl Values<'_> {
    pub fn extend(&mut self, other: Self) {
        match self {
            Self::String(v) => {
                if let Self::String(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
            Self::Float(v) => {
                if let Self::Float(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
            Self::Integer(v) => {
                if let Self::Integer(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
        }
    }
}

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Scalar {
    Float(f64),
    Integer(i64),
    Unsigned32(u32),
}

impl Scalar {
    pub fn reset(&mut self) {
        match self {
            Scalar::Float(v) => {
                *v = 0.0;
            }
            Scalar::Integer(v) => {
                *v = 0;
            }
            Scalar::Unsigned32(v) => {
                *v = 0;
            }
        }
    }

    pub fn add(&mut self, other: Scalar) {
        match self {
            Self::Float(v) => {
                if let Self::Float(other) = other {
                    *v += other;
                } else {
                    panic!("invalid");
                };
            }
            Self::Integer(v) => {
                if let Self::Integer(other) = other {
                    *v += other;
                } else {
                    panic!("invalid");
                };
            }
            Self::Unsigned32(v) => {
                if let Self::Unsigned32(other) = other {
                    *v += other;
                } else {
                    panic!("invalid");
                };
            }
        }
    }
}

impl<'a> std::ops::Add<&Scalar> for &mut Scalar {
    type Output = Scalar;

    fn add(self, _rhs: &Scalar) -> Self::Output {
        match *self {
            Scalar::Float(v) => {
                if let Scalar::Float(other) = _rhs {
                    Scalar::Float(v + other)
                } else {
                    panic!("invalid");
                }
            }
            Scalar::Integer(v) => {
                if let Scalar::Integer(other) = _rhs {
                    Scalar::Integer(v + other)
                } else {
                    panic!("invalid");
                }
            }
            Scalar::Unsigned32(v) => {
                if let Scalar::Unsigned32(other) = _rhs {
                    Scalar::Unsigned32(v + other)
                } else {
                    panic!("invalid");
                }
            }
        }
    }
}

impl<'a> std::ops::Add<&Scalar> for Scalar {
    type Output = Scalar;

    fn add(self, _rhs: &Scalar) -> Self::Output {
        match self {
            Self::Float(v) => {
                if let Self::Float(other) = _rhs {
                    Self::Float(v + other)
                } else {
                    panic!("invalid");
                }
            }
            Self::Integer(v) => {
                if let Self::Integer(other) = _rhs {
                    Self::Integer(v + other)
                } else {
                    panic!("invalid");
                }
            }
            Self::Unsigned32(v) => {
                if let Self::Unsigned32(other) = _rhs {
                    Self::Unsigned32(v + other)
                } else {
                    panic!("invalid");
                }
            }
        }
    }
}

impl<'a> std::ops::AddAssign<&Scalar> for &mut Scalar {
    fn add_assign(&mut self, _rhs: &Scalar) {
        match self {
            Scalar::Float(v) => {
                if let Scalar::Float(other) = _rhs {
                    *v += *other;
                } else {
                    panic!("invalid");
                };
            }
            Scalar::Integer(v) => {
                if let Scalar::Integer(other) = _rhs {
                    *v += *other;
                } else {
                    panic!("invalid");
                };
            }
            Scalar::Unsigned32(v) => {
                if let Scalar::Unsigned32(other) = _rhs {
                    *v += *other;
                } else {
                    panic!("invalid");
                };
            }
        }
    }
}

impl<'a> std::ops::AddAssign<&Scalar> for Scalar {
    fn add_assign(&mut self, _rhs: &Scalar) {
        match self {
            Self::Float(v) => {
                if let Self::Float(other) = _rhs {
                    *v += *other;
                } else {
                    panic!("invalid");
                };
            }
            Self::Integer(v) => {
                if let Self::Integer(other) = _rhs {
                    *v += *other;
                } else {
                    panic!("invalid");
                };
            }
            Self::Unsigned32(v) => {
                if let Self::Unsigned32(other) = _rhs {
                    *v += *other;
                } else {
                    panic!("invalid");
                };
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Aggregate {
    Count(u64),
    // Sum can be `None` is for example all values being aggregated are themselves
    // `None`.
    Sum(Option<Scalar>),
}

#[derive(Debug, Clone)]
pub enum AggregateType {
    Count,
    Sum,
}

// impl<'a> std::ops::Add<&Option<Scalar>> for Aggregate {
//     type Output = Aggregate;

//     fn add(self, _rhs: &Option<Scalar>) -> Self::Output {
//         match self {
//             Self::Count(self_count) => match _rhs {
//                 Some(other_scalar) => match other_scalar {
//                     Scalar::String(_) => panic!("todo - remove String
// scalar"),                     Scalar::Float(_) => panic!("cannot add floating
// point value to a count"),                     Scalar::Integer(v) =>
// Self::Count(self_count + *v as u64),
// Scalar::Unsigned32(v) => Self::Count(self_count + *v as u64),
// },                 None => self,
//             },
//             // SUM ignores NULL values. Initially an aggregate sum is `None`,
// but             // as soon as a non-null value is shown then it becomes
// `Some`.             Self::Sum(self_sum) => match (self_sum, _rhs) {
//                 (None, None) => Self::Sum(None),
//                 (None, Some(other_scalar)) => match other_scalar {
//                     Scalar::String(_) => panic!("todo - remove String
// scalar"),                     Scalar::Float(_) =>
// Self::Sum(Some(other_scalar.clone())),                     Scalar::Integer(_)
// => Self::Sum(Some(other_scalar.clone())),
// Scalar::Unsigned32(_) => Self::Sum(Some(other_scalar.clone())),
// },                 (Some(_self), None) => Self::Sum(Some(_self.clone())),
//                 (Some(self_scalar), Some(other_scalar)) => match other_scalar
// {                     Scalar::String(_) => panic!("todo - remove String
// scalar"),                     Scalar::Float(_) => Self::Sum(Some(self_scalar
// + &other_scalar)),                     Scalar::Integer(_) =>
// Self::Sum(Some(self_scalar + &other_scalar)),
// Scalar::Unsigned32(_) => Self::Sum(Some(self_scalar + &other_scalar)),
//                 },
//             },
//         }
//     }
// }

// impl<'a> std::ops::Add<&Aggregate> for Aggregate {
//     type Output = Aggregate;

//     fn add(self, _rhs: &Aggregate) -> Self::Output {
//         match self {
//             Self::Count(self_count) => {
//                 if let Self::Count(other) = _rhs {
//                     Self::Count(self_count + *other)
//                 } else {
//                     panic!("can't combine count with other aggregate type");
//                 }
//             }
//             // SUM ignores NULL values. Initially an aggregate sum is `None`,
// but             // as soon as a non-null value is shown then it becomes
// `Some`.             Self::Sum(self_sum) => {
//                 if let Self::Sum(other) = _rhs {
//                     match (self_sum, other) {
//                         (None, None) => Self::Sum(None),
//                         (None, Some(_)) => Self::Sum(*other),
//                         (Some(_), None) => self,
//                         (Some(s), Some(other)) => Self::Sum(Some(s + other)),
//                     }
//                 } else {
//                     panic!("invalid");
//                 }
//             }
//         }
//     }
// }

pub trait AggregatableByRange {
    fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate;
}

/// A Vector is a materialised vector of scalar values from a column.
pub enum Vector {
    // NullString(Vec<&'a Option<std::string::String>>),
    NullFloat(Vec<Option<f64>>),
    NullInteger(Vec<Option<i64>>),

    Float(Vec<f64>),
    Integer(Vec<i64>),
    Unsigned32(Vec<u32>),
    /* TODO(edd): add types like this:
     *
     * Integer16(Vec<i16>),
     * NullInteger16(Vec<Option<i16>>), // contains one or more NULL values
     * ...
     * ...
     *
     * We won't need EncodedString then (it can use one of the non-null integer variants)
     */
}

impl Vector {
    pub fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate {
        match agg_type {
            AggregateType::Count => {
                Aggregate::Count(self.count_by_id_range(from_row_id, to_row_id))
            }
            AggregateType::Sum => Aggregate::Sum(self.sum_by_id_range(from_row_id, to_row_id)),
        }
    }

    // Return the sum of values in the vector. NULL values are ignored. If there
    // are no non-null values in the vector being summed then None is returned.
    fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> Option<Scalar> {
        match self {
            // Self::NullString(_) => {
            //     panic!("can't sum strings....");
            // }
            Self::NullFloat(values) => {
                let mut res = 0.0;
                let mut found = false; // TODO(edd): check if this is faster than a match.

                // TODO(edd): check asm to see if it's vectorising
                for v in values[from_row_id..to_row_id].iter().flatten() {
                    res += *v;
                    found = true;
                }

                if found {
                    return Some(Scalar::Float(res));
                }
                None
            }
            Self::Float(values) => {
                let mut res = 0.0;

                // TODO(edd): check asm to see if it's vectorising
                for v in values[from_row_id..to_row_id].iter() {
                    res += *v;
                }
                Some(Scalar::Float(res))
            }
            Self::NullInteger(values) => {
                let mut res = 0;
                let mut found = false;

                // TODO(edd): check asm to see if it's vectorising
                for v in values[from_row_id..to_row_id].iter().flatten() {
                    res += *v;
                    found = true;
                }

                if found {
                    return Some(Scalar::Integer(res));
                }
                None
            }
            Self::Integer(values) => {
                let mut res = 0;

                // TODO(edd): check asm to see if it's vectorising
                for v in values[from_row_id..to_row_id].iter() {
                    res += *v;
                }
                Some(Scalar::Integer(res))
            }
            Self::Unsigned32(values) => {
                let mut res = 0;

                // TODO(edd): check asm to see if it's vectorising
                for v in values[from_row_id..to_row_id].iter() {
                    res += *v;
                }
                Some(Scalar::Unsigned32(res))
            }
        }
    }

    // return the count of values on the column. NULL values do not contribute
    // to the count.
    fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> u64 {
        match self {
            // Self::NullString(vec) => {
            //     let mut count = 0;
            //     for v in &vec[from_row_id..to_row_id] {
            //         if v.is_some() {
            //             count += 1;
            //         }
            //     }
            //     count as u64
            // }
            Self::NullFloat(vec) => vec[from_row_id..to_row_id].iter().flatten().count() as u64,
            Self::NullInteger(vec) => vec[from_row_id..to_row_id].iter().flatten().count() as u64,
            Self::Float(_) => {
                (to_row_id - from_row_id) as u64 // fast - no possible NULL
                                                 // values
            }
            Self::Integer(_) => {
                (to_row_id - from_row_id) as u64 // fast - no possible NULL
                                                 // values
            }
            Self::Unsigned32(_) => {
                (to_row_id - from_row_id) as u64 // fast - no possible NULL
                                                 // values
            }
        }
    }

    pub fn extend(&mut self, other: Self) {
        match self {
            // Self::NullString(v) => {
            //     if let Self::NullString(other) = other {
            //         v.extend(other);
            //     } else {
            //         unreachable!("string can't be extended");
            //     }
            // }
            Self::NullFloat(v) => {
                if let Self::NullFloat(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
            Self::NullInteger(v) => {
                if let Self::NullInteger(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
            Self::Float(v) => {
                if let Self::Float(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
            Self::Integer(v) => {
                if let Self::Integer(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
            Self::Unsigned32(v) => {
                if let Self::Unsigned32(other) = other {
                    v.extend(other);
                } else {
                    unreachable!("string can't be extended");
                }
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            // Self::NullString(v) => v.len(),
            Self::NullFloat(v) => v.len(),
            Self::NullInteger(v) => v.len(),
            Self::Float(v) => v.len(),
            Self::Integer(v) => v.len(),
            Self::Unsigned32(v) => v.len(),
        }
    }

    /// Return the value within the vector at position `i`. If the value at
    /// position `i` is NULL then `None` is returned.
    pub fn get(&self, i: usize) -> Option<Scalar> {
        match self {
            // Self::NullString(v) => match v[i] {
            //     Some(v) => Value::String(v),
            //     None => Value::Null, // Scalar::String(v[i].as_ref().unwrap()),
            // },
            Self::NullFloat(v) => v[i].map(Scalar::Float),
            Self::NullInteger(v) => v[i].map(Scalar::Integer),
            Self::Float(v) => Some(Scalar::Float(v[i])),
            Self::Integer(v) => Some(Scalar::Integer(v[i])),
            Self::Unsigned32(v) => Some(Scalar::Unsigned32(v[i])),
        }
    }

    /// Return the value within the vector at position `i`. If the value at
    /// position `i` is NULL then `None` is returned.
    //
    // TODO - sort out
    // pub fn get_scalar(&self, i: usize) -> Option<Scalar> {
    //     match self {
    //         Self::NullString(_) => panic!("unsupported get_scalar"),
    //         Self::NullFloat(v) => match v[i] {
    //             Some(v) => Some(Scalar::Float(v)),
    //             None => None,
    //         },
    //         Self::NullInteger(v) => match v[i] {
    //             Some(v) => Some(Scalar::Integer(v)),
    //             None => None,
    //         },
    //         Self::Float(v) => Some(Scalar::Float(v[i])),
    //         Self::Integer(v) => Some(Scalar::Integer(v[i])),
    //         Self::Unsigned32(v) => Some(Scalar::Unsigned32(v[i])),
    //     }
    // }

    pub fn swap(&mut self, a: usize, b: usize) {
        match self {
            // Self::NullString(v) => {
            //     v.swap(a, b);
            // }
            Self::NullFloat(v) => {
                v.swap(a, b);
            }
            Self::NullInteger(v) => {
                v.swap(a, b);
            }
            Self::Float(v) => {
                v.swap(a, b);
            }
            Self::Integer(v) => {
                v.swap(a, b);
            }
            Self::Unsigned32(v) => {
                v.swap(a, b);
            }
        }
    }
}

impl AggregatableByRange for Vector {
    fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate {
        Vector::aggregate_by_id_range(&self, agg_type, from_row_id, to_row_id)
    }
}

pub struct VectorIterator {
    v: Vector,
    next_i: usize,
}

impl VectorIterator {
    pub fn new(v: Vector) -> Self {
        Self { v, next_i: 0 }
    }
}
impl Iterator for VectorIterator {
    type Item = Option<Scalar>;

    fn next(&mut self) -> Option<Self::Item> {
        let curr_i = self.next_i;
        self.next_i += 1;

        if curr_i == self.v.len() {
            return None;
        }

        Some(self.v.get(curr_i))
    }
}

/// NullVectorIterator allows a `Vector` to be iterated. Until vectors are
/// drained Scalar values are emitted.
///
///
/// TODO - need to figure this out - currently only returns scalars
// pub struct NullVectorIterator {
//     v: Vector,
//     next_i: usize,
// }

// impl NullVectorIterator {
//     pub fn new(v: Vector) -> Self {
//         Self { v, next_i: 0 }
//     }
// }
// impl Iterator for NullVectorIterator {
//     type Item = Option<Scalar>;

//     fn next(&mut self) -> Option<Self::Item> {
//         let curr_i = self.next_i;
//         self.next_i += 1;

//         if curr_i == self.v.len() {
//             return None;
//         }

//         Some(self.v.get_scalar(curr_i))
//     }
// }
use chrono::prelude::*;

impl std::fmt::Display for Vector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // Self::NullString(v) => write!(f, "{:?}", v),
            Self::NullFloat(v) => write!(f, "{:?}", v),
            Self::NullInteger(v) => {
                for x in v.iter() {
                    match x {
                        Some(x) => {
                            let ts = NaiveDateTime::from_timestamp(*x / 1000 / 1000, 0);
                            write!(f, "{}, ", ts)?;
                        }
                        None => write!(f, "NULL, ")?,
                    }
                }
                Ok(())
            }
            Self::Float(v) => write!(f, "{:?}", v),
            Self::Integer(v) => {
                // TODO(edd) remove as this is timestamp specific
                for x in v.iter() {
                    let ts = NaiveDateTime::from_timestamp(*x / 1000 / 1000, 0);
                    write!(f, "{}, ", ts)?;
                }
                Ok(())
            }
            Self::Unsigned32(v) => write!(f, "{:?}", v),
        }
    }
}

#[derive(Debug)]
pub enum Column {
    String(String),
    Float(NumericColumn<f64>),
    Integer(NumericColumn<i64>),
}

impl Column {
    /// Returns the number of logical rows for the column.
    pub fn num_rows(&self) -> usize {
        match self {
            Column::String(c) => c.meta.num_rows(),
            Column::Float(c) => c.meta.num_rows(),
            Column::Integer(c) => c.meta.num_rows(),
        }
    }

    // Returns the size of the column in bytes.
    pub fn size(&self) -> usize {
        match self {
            Column::String(c) => c.size(),
            Column::Float(c) => c.size(),
            Column::Integer(c) => c.size(),
        }
    }

    /// Materialise the decoded value matching the provided logical
    /// row id.
    pub fn value(&'_ self, row_id: usize) -> Value<'_> {
        match self {
            Column::String(c) => {
                if row_id >= self.num_rows() {
                    return Value::Null;
                }

                match c.value(row_id) {
                    Some(v) => Value::String(v),
                    None => Value::Null,
                }
            }
            Column::Float(c) => {
                if row_id >= self.num_rows() {
                    return Value::Null;
                }

                let v = c.value(row_id);
                if let Some(v) = v {
                    return Value::Scalar(Scalar::Float(v));
                }
                Value::Null
            }
            Column::Integer(c) => {
                if row_id >= self.num_rows() {
                    return Value::Null;
                }

                let v = c.value(row_id);
                if let Some(v) = v {
                    return Value::Scalar(Scalar::Integer(v));
                }
                Value::Null
            }
        }
    }

    /// Materialise all of the decoded values matching the provided logical
    /// row ids.
    //
    // FIXME(edd): we need to provide an API on an encoding to return raw_values
    // so that we can return non-null vectors when we know the underlying encoding
    // doesn't contain any null values. Right now we return nullable vectors, w
    // which take up more memory and mean we can't do fast counts (since we need
    // to check each value is non-null).
    //
    pub fn values(&self, row_ids: &[usize]) -> Vector {
        match self {
            Column::String(_) => {
                panic!("unsupported at the moment")
                // if row_ids.is_empty() {
                //     return Vector::NullString(vec![]);
                // }

                // Vector::NullString(c.values(row_ids))
            }
            Column::Float(c) => {
                if row_ids.is_empty() {
                    return Vector::Float(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.values(row_ids);
                debug!("time getting decoded values for float {:?}", now.elapsed());

                Vector::NullFloat(v)
            }
            Column::Integer(c) => {
                if row_ids.is_empty() {
                    return Vector::Integer(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.values(row_ids);
                debug!("time getting decoded values for int {:?}", now.elapsed());
                Vector::NullInteger(v)
            }
        }
    }

    /// Materialise all of the decoded values matching the provided logical
    /// row ids within the bitmap
    pub fn values_bitmap(&self, row_ids: &croaring::Bitmap) -> Vector {
        match self {
            Column::String(_) => {
                unreachable!("unsupported at the moment");
                // if row_ids.is_empty() {
                //     return Vector::NullString(vec![]);
                // }

                // let row_id_vec = row_ids
                //     .to_vec()
                //     .iter()
                //     .map(|v| *v as usize)
                //     .collect::<Vec<_>>();
                // Vector::NullString(c.values(&row_id_vec))
            }
            Column::Float(c) => {
                if row_ids.is_empty() {
                    return Vector::Float(vec![]);
                }

                let row_id_vec = row_ids
                    .to_vec()
                    .iter()
                    .map(|v| *v as usize)
                    .collect::<Vec<_>>();
                Vector::NullFloat(c.values(&row_id_vec))
            }
            Column::Integer(c) => {
                if row_ids.is_empty() {
                    return Vector::Integer(vec![]);
                }

                let row_id_vec = row_ids
                    .to_vec()
                    .iter()
                    .map(|v| *v as usize)
                    .collect::<Vec<_>>();
                Vector::NullInteger(c.values(&row_id_vec))
            }
        }
    }

    /// Materialise all of the encoded values matching the provided logical
    /// row ids.
    pub fn encoded_values_bitmap(&self, row_ids: &croaring::Bitmap) -> Vector {
        let now = std::time::Instant::now();
        let row_ids_vec = row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        debug!("time unpacking bitmap {:?}", now.elapsed());

        match self {
            Column::String(c) => {
                if row_ids.is_empty() {
                    return Vector::Unsigned32(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.encoded_values(&row_ids_vec);
                debug!("time getting encoded values {:?}", now.elapsed());
                Vector::Unsigned32(v)
            }
            Column::Float(c) => {
                if row_ids.is_empty() {
                    return Vector::Float(vec![]);
                }

                Vector::Float(c.encoded_values(&row_ids_vec))
            }
            Column::Integer(c) => {
                if row_ids.is_empty() {
                    return Vector::Integer(vec![]);
                }

                Vector::Integer(c.encoded_values(&row_ids_vec))
            }
        }
    }

    /// Materialise all of the encoded values matching the provided logical
    /// row ids.
    pub fn encoded_values(&self, row_ids: &[usize]) -> Vector {
        match self {
            Column::String(c) => {
                if row_ids.is_empty() {
                    return Vector::Unsigned32(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.encoded_values(&row_ids);
                debug!("time getting encoded values {:?}", now.elapsed());

                debug!("dictionary {:?}", c.data.dictionary());
                Vector::Unsigned32(v)
            }
            Column::Float(c) => {
                if row_ids.is_empty() {
                    return Vector::Float(vec![]);
                }

                Vector::Float(c.encoded_values(&row_ids))
            }
            Column::Integer(c) => {
                if row_ids.is_empty() {
                    return Vector::Integer(vec![]);
                }

                Vector::Integer(c.encoded_values(&row_ids))
            }
        }
    }

    /// Materialise all of the encoded values.
    pub fn all_encoded_values(&self) -> Vector {
        match self {
            Column::String(c) => {
                let now = std::time::Instant::now();
                let v = c.all_encoded_values();
                debug!("time getting all encoded values {:?}", now.elapsed());

                debug!("dictionary {:?}", c.data.dictionary());
                Vector::Unsigned32(v)
            }
            Column::Float(c) => Vector::Float(c.all_encoded_values()),
            Column::Integer(c) => Vector::Integer(c.all_encoded_values()),
        }
    }

    /// Given an encoded value for a row, materialise and return the decoded
    /// version.
    ///
    /// This currently just supports decoding integer scalars back into
    /// dictionary strings.
    pub fn decode_value(&self, encoded_id: i64) -> std::string::String {
        match self {
            Column::String(c) => {
                // FIX THIS UNWRAP AND HOPE THERE ARE NO NULL VALUES!
                c.decode_id(encoded_id).unwrap()
            }
            Column::Float(_c) => {
                unreachable!("this isn't supported right now");
            }
            Column::Integer(_c) => {
                unreachable!("this isn't supported right now");
            }
        }
    }

    /// materialise rows for each row_id
    pub fn rows(&self, row_ids: &croaring::Bitmap) -> Values<'_> {
        let now = std::time::Instant::now();
        let row_ids_vec = row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        debug!("time unpacking bitmap {:?}", now.elapsed());

        assert!(
            row_ids_vec.len() == 1 || row_ids_vec[row_ids_vec.len() - 1] > row_ids_vec[0],
            "got last row_id={:?} and first row_id={:?}",
            row_ids_vec[row_ids_vec.len() - 1],
            row_ids_vec[0]
        );
        match self {
            Column::String(c) => Values::String(c.values(&row_ids_vec)),
            Column::Float(c) => Values::Float(c.values(&row_ids_vec)),
            Column::Integer(c) => Values::Integer(c.values(&row_ids_vec)),
        }
    }

    pub fn maybe_contains(&self, value: &Value<'_>) -> bool {
        match self {
            Column::String(c) => {
                if let Value::String(v) = value {
                    c.meta.maybe_contains_value(v.to_string())
                } else {
                    panic!("a String column cannot contain a non-string value");
                }
            }
            Column::Float(c) => {
                if let Value::Scalar(Scalar::Float(v)) = value {
                    c.meta.maybe_contains_value(*v)
                } else {
                    panic!("invalid value");
                }
            }
            Column::Integer(c) => {
                if let Value::Scalar(Scalar::Integer(v)) = value {
                    c.meta.maybe_contains_value(*v)
                } else {
                    panic!("invalid value");
                }
            }
        }
    }

    /// returns true if the column cannot contain
    pub fn max_less_than(&self, value: &Value<'_>) -> bool {
        match self {
            Column::String(c) => {
                if let Value::String(v) = value {
                    if let Some(range) = c.meta.range() {
                        &range.1.as_str() < v
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
            Column::Float(c) => {
                if let Value::Scalar(Scalar::Float(v)) = value {
                    if let Some(range) = c.meta.range() {
                        range.1 < *v
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
            Column::Integer(c) => {
                if let Value::Scalar(Scalar::Integer(v)) = value {
                    if let Some(range) = c.meta.range() {
                        range.1 < *v
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
        }
    }

    // TODO(edd): consolodate with max_less_than... Should just be single cmp
    // function
    pub fn min_greater_than(&self, value: &Value<'_>) -> bool {
        match self {
            Column::String(c) => {
                if let Value::String(v) = value {
                    if let Some(range) = c.meta.range() {
                        &range.0.as_str() > v
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
            Column::Float(c) => {
                if let Value::Scalar(Scalar::Float(v)) = value {
                    if let Some(range) = c.meta.range() {
                        range.0 > *v
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
            Column::Integer(c) => {
                if let Value::Scalar(Scalar::Integer(v)) = value {
                    if let Some(range) = c.meta.range() {
                        range.0 > *v
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
        }
    }

    /// Returns the minimum value contained within this column.
    pub fn min(&self) -> Value<'_> {
        match self {
            Column::String(c) => match c.meta.range() {
                Some(range) => Value::String(&range.0),
                None => Value::Null,
            },
            Column::Float(c) => match c.meta.range() {
                Some(range) => Value::Scalar(Scalar::Float(range.0)),
                None => Value::Null,
            },
            Column::Integer(c) => match c.meta.range() {
                Some(range) => Value::Scalar(Scalar::Integer(range.0)),
                None => Value::Null,
            },
        }
    }

    /// Returns the maximum value contained within this column.
    // FIXME(edd): Support NULL integers and floats
    pub fn max(&self) -> Value<'_> {
        match self {
            Column::String(c) => match c.meta.range() {
                Some(range) => Value::String(&range.1),
                None => Value::Null,
            },
            Column::Float(c) => match c.meta.range() {
                Some(range) => Value::Scalar(Scalar::Float(range.1)),
                None => Value::Null,
            },
            Column::Integer(c) => match c.meta.range() {
                Some(range) => Value::Scalar(Scalar::Integer(range.1)),
                None => Value::Null,
            },
        }
    }

    pub fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> Option<Scalar> {
        match self {
            Column::String(_) => unimplemented!("not implemented"),
            Column::Float(c) => c.sum_by_ids(row_ids).map(Scalar::Float),
            Column::Integer(_) => unimplemented!("not implemented"),
        }
    }

    pub fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate {
        match self {
            Column::String(_) => unimplemented!("not implemented"),
            Column::Float(c) => match agg_type {
                AggregateType::Count => {
                    Aggregate::Count(c.count_by_id_range(from_row_id, to_row_id))
                }
                AggregateType::Sum => match c.sum_by_id_range(from_row_id, to_row_id) {
                    Some(sum) => Aggregate::Sum(Some(Scalar::Float(sum))),
                    None => Aggregate::Sum(None),
                },
            },

            Column::Integer(_) => unimplemented!("not implemented"),
        }
    }

    pub fn group_by_ids(&self) -> &std::collections::BTreeMap<u32, croaring::Bitmap> {
        match self {
            Column::String(c) => c.data.group_row_ids(),
            Column::Float(_) => unimplemented!("not implemented"),
            Column::Integer(_) => unimplemented!("not implemented"),
        }
    }

    // TODO(edd) shouldn't let roaring stuff leak out...
    pub fn row_ids_eq(&self, value: &Value<'_>) -> Option<croaring::Bitmap> {
        if let Value::Null = value {
            return None; // don't support "IS NULL" yet.
        }

        if !self.maybe_contains(value) {
            return None;
        }
        Some(self.row_ids(value, std::cmp::Ordering::Equal))
    }

    pub fn row_ids_gt(&self, value: &Value<'_>) -> Option<croaring::Bitmap> {
        if self.max_less_than(value) {
            return None;
        }
        Some(self.row_ids(value, std::cmp::Ordering::Greater))
    }

    pub fn row_ids_lt(&self, value: &Value<'_>) -> Option<croaring::Bitmap> {
        if self.min_greater_than(value) {
            return None;
        }
        Some(self.row_ids(value, std::cmp::Ordering::Less))
    }

    // allows you to do:
    //      WHERE time >= 0 AND time < 100
    //
    // or
    //
    //      WHERE counter >= 102.2 AND counter < 2929.32
    pub fn row_ids_gte_lt(&self, low: &Value<'_>, high: &Value<'_>) -> Option<croaring::Bitmap> {
        if let (Value::Null, _) | (_, Value::Null) = (low, high) {
            panic!("unsupported NULL value in range");
        }

        match self {
            Column::String(_c) => {
                unimplemented!("not implemented yet");
            }
            Column::Float(c) => {
                let (col_min, col_max) = match c.meta.range() {
                    Some(range) => range,
                    // no min/max on column which means must be all NULL values.
                    None => return None,
                };

                if let (Value::Scalar(Scalar::Float(low)), Value::Scalar(Scalar::Float(high))) =
                    (low, high)
                {
                    if low <= col_min && high > col_max {
                        // In this case the query completely covers the range of the column.
                        // TODO: PERF - need to _not_ return a bitset rather than
                        // return a full one. Need to differentiate between "no values"
                        // and "all values" in the context of an Option. Right now
                        // None means "no values"
                        //
                        let mut bm = croaring::Bitmap::create();
                        bm.add_range(0..c.meta.num_rows() as u64); // all rows
                        return Some(bm);
                    }

                    // The column has some values that are outside of the
                    // desired range so we need to determine the set of matching
                    // row ids.
                    Some(c.data.row_ids_gte_lt_roaring(low, high))
                } else {
                    panic!("not supposed to be here");
                }
            }
            Column::Integer(c) => {
                let (col_min, col_max) = match c.meta.range() {
                    Some(range) => range,
                    // no min/max on column which means must be all NULL values.
                    None => return None,
                };

                if let (Value::Scalar(Scalar::Integer(low)), Value::Scalar(Scalar::Integer(high))) =
                    (low, high)
                {
                    if low <= col_min && high > col_max {
                        // In this case the query completely covers the range of the column.
                        // TODO: PERF - need to _not_ return a bitset rather than
                        // return a full one. Need to differentiate between "no values"
                        // and "all values" in the context of an Option. Right now
                        // None means "no values"
                        //
                        let mut bm = croaring::Bitmap::create();
                        bm.add_range(0..c.meta.num_rows() as u64); // all rows
                        return Some(bm);
                    }

                    // The column has some values that are outside of the
                    // desired range so we need to determine the set of matching
                    // row ids.
                    Some(c.data.row_ids_gte_lt_roaring(low, high))
                } else {
                    panic!("not supposed to be here");
                }
            }
        }
    }

    // TODO(edd) shouldn't let roaring stuff leak out...
    fn row_ids(&self, value: &Value<'_>, order: std::cmp::Ordering) -> croaring::Bitmap {
        match self {
            Column::String(c) => {
                if order != std::cmp::Ordering::Equal {
                    unimplemented!("> < not supported on strings yet");
                }

                if let Value::String(v) = value {
                    c.data.row_ids_eq_roaring(Some(v.to_string()))
                } else {
                    panic!("invalid value");
                }
            }
            Column::Float(c) => {
                if let Value::Scalar(Scalar::Float(v)) = value {
                    c.data.row_ids_single_cmp_roaring(v, order)
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Value::Scalar(Scalar::Integer(v)) = value {
                    c.data.row_ids_single_cmp_roaring(v, order)
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
        }
    }

    // great catchy name... This determines as efficiently as possible if the
    // column contains a non-null value in at least one of the provided row
    // ids.
    //
    // row_ids *must* be in ascending order.
    pub fn has_non_null_value_in_row_ids(&self, row_ids: &[usize]) -> bool {
        match self {
            Column::String(c) => c.data.has_non_null_value_in_row_ids(row_ids),
            _ => unreachable!("not supported at the moment"),
        }
    }

    /// This returns the distinct set of values in the column from the set of
    /// rows provided.
    ///
    /// NULL values are not included in the returned set even if present in the
    /// column at provided rows.
    ///
    /// row_ids *must* be in ascending order.
    pub fn distinct_values(&self, row_ids: &[usize]) -> Set<'_> {
        match self {
            Column::String(c) => Set::String(c.data.distinct_values(row_ids)),
            _ => unreachable!("not supported at the moment"),
        }
    }

    /// Returns true if the column contains any values other than those in
    /// `values`.
    pub fn contains_other_values(&self, values: &BTreeSet<&std::string::String>) -> bool {
        match self {
            Column::String(c) => {
                // TODO(edd):
                // had problems with ref inside of enum Set variant.

                // if let Set::String(v) = values {
                c.data.contains_other_values(values)
                // } else {
                // panic!("incompatible set with column type");
                // }
                // Set::String(c.data.distinct_values(row_ids))
            }
            _ => unreachable!("not supported at the moment"),
        }
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Column::String(c) => {
                write!(f, "[String Column]: {}", c)?;
            }
            Column::Float(c) => {
                write!(f, "[Float Column]:{}", c)?;
            }
            Column::Integer(c) => {
                write!(f, "[Integer Column]: {}", c)?;
            }
        }
        Ok(())
    }
}

impl AggregatableByRange for &Column {
    fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate {
        Column::aggregate_by_id_range(&self, agg_type, from_row_id, to_row_id)
    }
}

impl From<arrow::array::Float64Array> for Column {
    fn from(arr: arrow::array::Float64Array) -> Self {
        Self::Float(NumericColumn::from(arr))
    }
}

impl From<arrow::array::TimestampMicrosecondArray> for Column {
    fn from(arr: arrow::array::TimestampMicrosecondArray) -> Self {
        Self::Integer(NumericColumn::from(arr))
    }
}

impl From<arrow::array::Int64Array> for Column {
    fn from(arr: arrow::array::Int64Array) -> Self {
        Self::Integer(NumericColumn::from(arr))
    }
}

impl From<&[f64]> for Column {
    fn from(values: &[f64]) -> Self {
        Self::Float(NumericColumn::from(values))
    }
}

impl From<&[i64]> for Column {
    fn from(values: &[i64]) -> Self {
        Self::Integer(NumericColumn::from(values))
    }
}

#[derive(Debug, Default)]
pub struct String {
    meta: metadata::Metadata<std::string::String>,

    // TODO(edd): this would probably have multiple possible encodings
    data: encoding::DictionaryRle,
}

impl String {
    pub fn with_dictionary(
        dictionary: std::collections::BTreeSet<Option<std::string::String>>,
    ) -> Self {
        Self {
            data: encoding::DictionaryRle::with_dictionary(dictionary),
            ..Default::default()
        }
    }

    pub fn add(&mut self, s: &str) {
        self.meta.add(Some(s.to_string()));
        self.data.push(s);
    }

    pub fn add_additional(&mut self, s: Option<std::string::String>, additional: u64) {
        self.meta.add_repeated(s.clone(), additional as usize);
        self.data.push_additional(s, additional);
    }

    pub fn column_range(&self) -> &Option<(std::string::String, std::string::String)> {
        self.meta.range()
    }

    pub fn size(&self) -> usize {
        self.meta.size() + self.data.size()
    }

    pub fn value(&self, row_id: usize) -> Option<&std::string::String> {
        self.data.value(row_id)
    }

    pub fn values(&self, row_ids: &[usize]) -> Vec<&Option<std::string::String>> {
        self.data.values(row_ids)
    }

    pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<u32> {
        self.data.encoded_values(row_ids)
    }

    pub fn all_encoded_values(&self) -> Vec<u32> {
        self.data.all_encoded_values()
    }

    /// Return the decoded value for an encoded ID.
    ///
    /// Panics if there is no decoded value for the provided id
    pub fn decode_id(&self, encoded_id: i64) -> Option<std::string::String> {
        self.data.decode_id(encoded_id as usize)
    }

    pub fn scan_from(&self, row_id: usize) -> Vec<&Option<std::string::String>> {
        self.data.scan_from(row_id)
    }

    // TODO(edd) shouldn't let roaring stuff leak out...
    pub fn group_row_ids(&self) -> &std::collections::BTreeMap<u32, croaring::Bitmap> {
        self.data.group_row_ids()
    }
}

impl std::fmt::Display for String {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Meta: {}, Data: {}", self.meta, self.data)
    }
}

// #[derive(Debug)]
// pub struct Float {
//     meta: metadata::F64,

//     // TODO(edd): compression of float columns
//     // data: encoding::PlainFixed<f64>,
//     data: Box<dyn encoding::NumericEncoding<Item = f64>>,
// }

// impl Float {
//     pub fn column_range(&self) -> (f64, f64) {
//         self.meta.range()
//     }

//     pub fn size(&self) -> usize {
//         self.meta.size() + self.data.size()
//     }

//     pub fn value(&self, row_id: usize) -> f64 {
//         self.data.value(row_id)
//     }

//     pub fn values(&self, row_ids: &[usize]) -> Vec<f64> {
//         self.data.values(row_ids)
//     }

//     pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<f64> {
//         self.data.encoded_values(row_ids)
//     }

//     pub fn all_encoded_values(&self) -> Vec<f64> {
//         self.data.all_encoded_values()
//     }

//     pub fn scan_from(&self, row_id: usize) -> &[f64] {
//         self.data.scan_from(row_id)
//     }

//     pub fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> f64 {
//         self.data.sum_by_ids(row_ids)
//     }

//     pub fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) ->
// f64 {         self.data.sum_by_id_range(from_row_id, to_row_id)
//     }

//     pub fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) ->
// usize {         self.data.count_by_id_range(from_row_id, to_row_id)
//     }
// }

// impl std::fmt::Display for Float {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "Meta: {}, Data: {}", self.meta, self.data)
//     }
// }

// impl From<&[f64]> for Float {
//     fn from(values: &[f64]) -> Self {
//         let len = values.len();
//         let mut min = std::f64::MAX;
//         let mut max = std::f64::MIN;

//         // calculate min/max for meta data
//         for v in values {
//             min = min.min(*v);
//             max = max.max(*v);
//         }

//         Self {
//             meta: metadata::F64::new((min, max), len),
//             data: Box::new(encoding::PlainFixed::from(values)),
//         }
//     }
// }

// use arrow::array::Array;
// impl From<arrow::array::PrimitiveArray<arrow::datatypes::Float64Type>> for
// Float {     fn from(arr:
// arrow::array::PrimitiveArray<arrow::datatypes::Float64Type>) -> Self {
//         let len = arr.len();
//         let mut min = std::f64::MAX;
//         let mut max = std::f64::MIN;

//         // calculate min/max for meta data
//         // TODO(edd): can use compute kernels for this.
//         for i in 0..arr.len() {
//             if arr.is_null(i) {
//                 continue;
//             }

//             let v = arr.value(i);
//             min = min.min(v);
//             max = max.max(v);
//         }

//         Self {
//             meta: metadata::F64::new((min, max), len),
//             data: Box::new(encoding::PlainArrow { arr }),
//         }
//     }
// }

// #[derive(Debug)]
// pub struct Integer {
//     meta: metadata::Metadata<i64>,

//     // TODO(edd): compression of integers
//     data: Box<dyn encoding::NumericEncoding<Item = i64>>,
// }

// impl Integer {
//     pub fn column_range(&self) -> (Option<&i64>, Option<&i64>) {
//         self.meta.range()
//     }

//     pub fn size(&self) -> usize {
//         self.meta.size() + self.data.size()
//     }

//     pub fn value(&self, row_id: usize) -> i64 {
//         self.data.value(row_id)
//     }

//     pub fn values(&self, row_ids: &[usize]) -> Vec<i64> {
//         self.data.values(row_ids)
//     }

//     pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<i64> {
//         self.data.encoded_values(row_ids)
//     }

//     pub fn all_encoded_values(&self) -> Vec<i64> {
//         self.data.all_encoded_values()
//     }

//     pub fn scan_from(&self, row_id: usize) -> &[i64] {
//         self.data.scan_from(row_id)
//     }

//     /// Find the first logical row that contains this value.
//     pub fn row_id_eq_value(&self, v: i64) -> Option<usize> {
//         if !self.meta.maybe_contains_value(v) {
//             return None;
//         }
//         self.data.row_id_eq_value(v)
//     }
// }

// impl std::fmt::Display for Integer {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         write!(f, "Meta: {}, Data: {}", self.meta, self.data)
//     }
// }

// impl From<&[i64]> for Integer {
//     fn from(values: &[i64]) -> Self {
//         let len = values.len();
//         let mut min = std::i64::MAX;
//         let mut max = std::i64::MIN;

//         // calculate min/max for meta data
//         for v in values {
//             min = min.min(*v);
//             max = max.max(*v);
//         }

//         Self {
//             meta: metadata::Metadata::new((Some(min), Some(max)), len),
//             data: Box::new(encoding::PlainFixed::from(values)),
//         }
//     }
// }

#[derive(Debug)]
pub struct NumericColumn<T>
where
    T: Clone + std::cmp::PartialOrd + std::fmt::Debug,
{
    meta: metadata::Metadata<T>,

    // TODO(edd): compression of integers
    data: Box<dyn encoding::NumericEncoding<Item = T>>,
}

impl<T> NumericColumn<T>
where
    T: Copy + Clone + std::cmp::PartialOrd + std::fmt::Debug,
{
    pub fn column_range(&self) -> &Option<(T, T)> {
        self.meta.range()
    }

    pub fn size(&self) -> usize {
        self.meta.size() + self.data.size()
    }

    pub fn value(&self, row_id: usize) -> Option<T> {
        self.data.value(row_id)
    }

    pub fn values(&self, row_ids: &[usize]) -> Vec<Option<T>> {
        self.data.values(row_ids)
    }

    pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<T> {
        self.data.encoded_values(row_ids)
    }

    pub fn all_encoded_values(&self) -> Vec<T> {
        self.data.all_encoded_values()
    }

    pub fn scan_from(&self, row_id: usize) -> &[Option<T>] {
        self.data.scan_from(row_id)
    }

    /// Find the first logical row that contains this value.
    pub fn row_id_eq_value(&self, v: T) -> Option<usize> {
        if !self.meta.maybe_contains_value(v) {
            return None;
        }
        self.data.row_id_eq_value(v)
    }

    pub fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> Option<T> {
        self.data.sum_by_ids(row_ids)
    }

    pub fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> Option<T> {
        self.data.sum_by_id_range(from_row_id, to_row_id)
    }

    pub fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> u64 {
        self.data.count_by_id_range(from_row_id, to_row_id)
    }
}

impl<T> std::fmt::Display for NumericColumn<T>
where
    T: Clone + std::cmp::PartialOrd + std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Meta: {}, Data: {}", self.meta, self.data)
    }
}

use arrow::array::Array;

impl From<arrow::array::Float64Array> for NumericColumn<f64> {
    fn from(arr: arrow::array::Float64Array) -> Self {
        let len = arr.len();
        let mut range: Option<(f64, f64)> = None;

        // calculate min/max for meta data
        // TODO(edd): can use compute kernels for this.
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let v = arr.value(i);
            match range {
                Some(ref mut range) => {
                    range.0 = range.0.min(v);
                    range.1 = range.1.max(v);
                }
                None => {
                    range = Some((v, v));
                }
            }
        }

        Self {
            meta: metadata::Metadata::new(range, len),
            data: Box::new(encoding::PlainArrow::new(arr)),
        }
    }
}

impl From<arrow::array::Int64Array> for NumericColumn<i64> {
    fn from(arr: arrow::array::Int64Array) -> Self {
        let len = arr.len();
        let mut range: Option<(i64, i64)> = None;

        // calculate min/max for meta data
        // TODO(edd): can use compute kernels for this.
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let v = arr.value(i);
            match range {
                Some(ref mut range) => {
                    range.0 = range.0.min(v);
                    range.1 = range.1.max(v);
                }
                None => {
                    range = Some((v, v));
                }
            }
        }

        Self {
            meta: metadata::Metadata::new(range, len),
            data: Box::new(encoding::PlainArrow::new(arr)),
        }
    }
}

impl From<arrow::array::TimestampMicrosecondArray> for NumericColumn<i64> {
    fn from(arr: arrow::array::TimestampMicrosecondArray) -> Self {
        let len = arr.len();
        let mut range: Option<(i64, i64)> = None;

        // calculate min/max for meta data
        // TODO(edd): can use compute kernels for this.
        for i in 0..arr.len() {
            if arr.is_null(i) {
                continue;
            }

            let v = arr.value(i);
            match range {
                Some(ref mut range) => {
                    range.0 = range.0.min(v);
                    range.1 = range.1.max(v);
                }
                None => {
                    range = Some((v, v));
                }
            }
        }

        Self {
            meta: metadata::Metadata::new(range, len),
            data: Box::new(encoding::PlainArrow::new(arr)),
        }
    }
}

impl From<&[f64]> for NumericColumn<f64> {
    fn from(values: &[f64]) -> Self {
        let len = values.len();
        let mut range: Option<(f64, f64)> = None;

        // calculate min/max for meta data
        for &v in values {
            match range {
                // wow this ref totally confused me for a while. Without it
                // the code will compile fine but the range option will never
                // reflect changes because the tuple range will be a copy.
                Some(ref mut range) => {
                    range.0 = range.0.min(v);
                    range.1 = range.1.max(v);
                }
                None => {
                    range = Some((v, v));
                }
            }
        }

        Self {
            meta: metadata::Metadata::new(range, len),
            data: Box::new(encoding::PlainFixed::from(values)),
        }
    }
}

impl From<&[i64]> for NumericColumn<i64> {
    fn from(values: &[i64]) -> Self {
        let len = values.len();
        let mut range: Option<(i64, i64)> = None;

        // calculate min/max for meta data
        for &v in values {
            match range {
                Some(ref mut range) => {
                    range.0 = range.0.min(v);
                    range.1 = range.1.max(v);
                }
                None => {
                    range = Some((v, v));
                }
            }
        }

        Self {
            meta: metadata::Metadata::new(range, len),
            data: Box::new(encoding::PlainFixed::from(values)),
        }
    }
}

pub mod metadata {
    use std::mem::size_of;

    #[derive(Debug, Default)]
    pub struct Metadata<T>
    where
        T: Clone + std::fmt::Debug,
    {
        range: Option<(T, T)>,
        num_rows: usize,
    }

    impl<T> Metadata<T>
    where
        T: Clone + std::cmp::PartialOrd<T> + std::fmt::Debug,
    {
        pub fn new(range: Option<(T, T)>, rows: usize) -> Self {
            Self {
                range,
                num_rows: rows,
            }
        }

        fn update_range(&mut self, v: T) {
            match &mut self.range {
                Some(range) => {
                    if v < range.0 {
                        range.0 = v.clone();
                    }

                    if v > range.1 {
                        range.1 = v;
                    }
                }
                None => {
                    self.range = Some((v.clone(), v));
                }
            }
        }

        pub fn add(&mut self, v: Option<T>) {
            self.num_rows += 1;

            if let Some(v) = v {
                self.update_range(v);
            }
        }

        pub fn add_repeated(&mut self, v: Option<T>, additional: usize) {
            self.num_rows += additional;

            if let Some(v) = v {
                self.update_range(v);
            }
        }

        pub fn num_rows(&self) -> usize {
            self.num_rows
        }

        pub fn maybe_contains_value(&self, v: T) -> bool {
            match &self.range {
                Some(range) => range.0 <= v && v <= range.1,
                None => false,
            }
        }

        pub fn range(&self) -> &Option<(T, T)> {
            &self.range
        }

        pub fn size(&self) -> usize {
            // size of types for num_rows and range
            size_of::<usize>() + (2 * size_of::<Option<String>>())

            //
            //  TODO: figure out a way to specify that T must be able to
            // describe its runtime size.
            //
            // match &self.range {
            //     (None, None) => base_size,
            //     (Some(min), None) => base_size + min.len(),
            //     (None, Some(max)) => base_size + max.len(),
            //     (Some(min), Some(max)) => base_size + min.len() + max.len(),
            // }
            // base_size
        }
    }

    impl<T: Clone + std::fmt::Debug> std::fmt::Display for Metadata<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Range: ({:?})", self.range)
        }
    }

    // #[derive(Debug, Default)]
    // pub struct Str {
    //     range: (Option<String>, Option<String>),
    //     num_rows: usize,
    // }

    // impl Str {
    //     pub fn add(&mut self, s: Option<String>) {
    //         self.num_rows += 1;

    //         if s < self.range.0 {
    //             self.range.0 = s.clone();
    //         }

    //         if s > self.range.1 {
    //             self.range.1 = s;
    //         }
    //     }

    //     pub fn add_repeated(&mut self, s: Option<String>, additional: usize)
    // {         self.num_rows += additional;

    //         if s < self.range.0 {
    //             self.range.0 = s.clone();
    //         }

    //         if s > self.range.1 {
    //             self.range.1 = s;
    //         }
    //     }

    //     pub fn num_rows(&self) -> usize {
    //         self.num_rows
    //     }

    //     pub fn maybe_contains_value(&self, v: Option<String>) -> bool {
    //         self.range.0 <= v && v <= self.range.1
    //     }

    //     pub fn range(&self) -> (Option<&String>, Option<&String>) {
    //         (self.range.0.as_ref(), self.range.1.as_ref())
    //     }

    //     pub fn size(&self) -> usize {
    //         // size of types for num_rows and range
    //         let base_size = size_of::<usize>() + (2 *
    // size_of::<Option<String>>());         match &self.range {
    //             (None, None) => base_size,
    //             (Some(min), None) => base_size + min.len(),
    //             (None, Some(max)) => base_size + max.len(),
    //             (Some(min), Some(max)) => base_size + min.len() + max.len(),
    //         }
    //     }
    // }

    // impl std::fmt::Display for Str {
    //     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    //         write!(f, "Range: ({:?})", self.range)
    //     }
    // }

    // #[derive(Debug, Default)]
    // pub struct F64 {
    //     range: (f64, f64),
    //     num_rows: usize,
    // }

    // impl F64 {
    //     pub fn new(range: (f64, f64), rows: usize) -> Self {
    //         Self {
    //             range,
    //             num_rows: rows,
    //         }
    //     }

    //     pub fn maybe_contains_value(&self, v: f64) -> bool {
    //         let res = self.range.0 <= v && v <= self.range.1;
    //         debug!(
    //             "column with ({:?}) maybe contain {:?} -- {:?}",
    //             self.range,
    //             v,
    //             res
    //         );
    //         res
    //     }

    //     pub fn num_rows(&self) -> usize {
    //         self.num_rows
    //     }

    //     pub fn range(&self) -> (f64, f64) {
    //         self.range
    //     }

    //     pub fn size(&self) -> usize {
    //         size_of::<usize>() + (size_of::<(f64, f64)>())
    //     }
    // }

    // impl std::fmt::Display for F64 {
    //     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    //         write!(f, "Range: ({:?})", self.range)
    //     }
    // }

    // #[derive(Debug, Default)]
    // pub struct I64 {
    //     range: (i64, i64),
    //     num_rows: usize,
    // }

    // impl I64 {
    //     pub fn new(range: (i64, i64), rows: usize) -> Self {
    //         Self {
    //             range,
    //             num_rows: rows,
    //         }
    //     }

    //     pub fn maybe_contains_value(&self, v: i64) -> bool {
    //         self.range.0 <= v && v <= self.range.1
    //     }

    //     pub fn max(&self) -> i64 {
    //         self.range.1
    //     }

    //     pub fn num_rows(&self) -> usize {
    //         self.num_rows
    //     }

    //     pub fn range(&self) -> (i64, i64) {
    //         self.range
    //     }

    //     pub fn size(&self) -> usize {
    //         size_of::<usize>() + (size_of::<(i64, i64)>())
    //     }
    // }

    // impl std::fmt::Display for I64 {
    //     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    //         write!(f, "Range: ({:?})", self.range)
    //     }
    // }
}
