use std::convert::From;

use super::encoding;

#[derive(Debug, PartialEq, PartialOrd, Clone)]
pub enum Scalar<'a> {
    String(&'a str),
    Float(f64),
    Integer(i64),
}

impl<'a> Scalar<'a> {
    pub fn reset(&mut self) {
        match self {
            Scalar::String(_s) => {
                panic!("not supported");
            }
            Scalar::Float(v) => {
                *v = 0.0;
            }
            Scalar::Integer(v) => {
                *v = 0;
            }
        }
    }

    pub fn add(&mut self, other: Scalar<'a>) {
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
            Self::String(_) => {
                unreachable!("not possible to add strings");
            }
        }
    }
}

impl<'a> std::ops::Add<&Scalar<'a>> for Scalar<'a> {
    type Output = Scalar<'a>;

    fn add(self, _rhs: &Scalar<'a>) -> Self::Output {
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
            Self::String(_) => {
                unreachable!("not possible to add strings");
            }
        }
    }
}

impl<'a> std::ops::AddAssign<&Scalar<'a>> for Scalar<'a> {
    fn add_assign(&mut self, _rhs: &Scalar<'a>) {
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
            Self::String(_) => {
                unreachable!("not possible to add strings");
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum Aggregate<'a> {
    Count(u64),
    Sum(Option<Scalar<'a>>),
}

#[derive(Debug, Clone)]
pub enum AggregateType {
    Count,
    Sum,
}

// impl<'a> std::ops::Add<Scalar<'a>> for Aggregate<'a> {
//     type Output = Aggregate<'a>;

//     fn add(self, _rhs: Scalar<'a>) -> Self::Output {
//         match self {
//             Self::Count(c) => Self::Count(c + 1),
//             Self::Sum(s) => Self::Sum(s + &_rhs),
//         }
//     }
// }

impl<'a> std::ops::Add<&Aggregate<'a>> for Aggregate<'a> {
    type Output = Aggregate<'a>;

    fn add(self, _rhs: &Aggregate<'a>) -> Self::Output {
        match self {
            Self::Count(c) => {
                if let Self::Count(other) = _rhs {
                    Self::Count(c + other)
                } else {
                    panic!("invalid");
                }
            }
            Self::Sum(s) => {
                if let Self::Sum(other) = _rhs {
                    match (s, other) {
                        (None, None) => Self::Sum(None),
                        (None, Some(other)) => Self::Sum(Some(*other)),
                        (Some(s), None) => Self::Sum(Some(s)),
                        (Some(s), Some(other)) => Self::Sum(Some(s + other)),
                    }
                } else {
                    panic!("invalid");
                }
            }
        }
    }
}

pub trait AggregatableByRange {
    fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate<'_>;
}
/// A Vector is a materialised vector of values from a column.
pub enum Vector<'a> {
    String(Vec<&'a Option<std::string::String>>),
    EncodedString(Vec<i64>),
    Float(Vec<Option<f64>>),
    Integer(Vec<Option<i64>>),
}

impl<'a> Vector<'a> {
    // pub fn aggregate_by_id_range(
    //     &self,
    //     agg_type: &AggregateType,
    //     from_row_id: usize,
    //     to_row_id: usize,
    // ) -> Aggregate<'a> {
    //     match agg_type {
    //         AggregateType::Count => {
    //             Aggregate::Count(self.count_by_id_range(from_row_id, to_row_id) as u64)
    //         }
    //         AggregateType::Sum => Aggregate::Sum(self.sum_by_id_range(from_row_id, to_row_id)),
    //     }
    // }

    // fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> Scalar<'a> {
    //     match self {
    //         Vector::String(_) => {
    //             panic!("can't sum strings....");
    //         }
    //         Vector::Float(values) => {
    //             let mut res = 0.0;
    //             // TODO(edd): check asm to see if it's vectorising
    //             for v in values[from_row_id..to_row_id].iter() {
    //                 res += *v;
    //             }
    //             Scalar::Float(res)
    //         }
    //         Vector::Integer(values) => {
    //             let mut res = 0;
    //             // TODO(edd): check asm to see if it's vectorising
    //             for v in values[from_row_id..to_row_id].iter() {
    //                 res += *v;
    //             }
    //             Scalar::Integer(res)
    //         }
    //     }
    // }

    fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> usize {
        to_row_id - from_row_id
    }

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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        match self {
            Self::String(v) => v.len(),
            Self::Float(v) => v.len(),
            Self::Integer(v) => v.len(),
        }
    }

    pub fn get(&self, i: usize) -> Scalar<'a> {
        match self {
            // FIXME(edd): SORT THIS OPTION OUT
            Self::String(v) => Scalar::String(v[i].as_ref().unwrap()),
            Self::Float(v) => Scalar::Float(v[i]),
            Self::Integer(v) => Scalar::Integer(v[i]),
        }
    }

    pub fn swap(&mut self, a: usize, b: usize) {
        match self {
            Self::String(v) => {
                v.swap(a, b);
            }
            Self::Float(v) => {
                v.swap(a, b);
            }
            Self::Integer(v) => {
                v.swap(a, b);
            }
        }
    }
}

impl AggregatableByRange for &Vector<'_> {
    fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate<'_> {
        Vector::aggregate_by_id_range(&self, agg_type, from_row_id, to_row_id)
    }
}

/// VectorIterator allows a `Vector` to be iterated. Until vectors are drained
/// Scalar values are emitted.
pub struct VectorIterator<'a> {
    v: &'a Vector<'a>,
    next_i: usize,
}

impl<'a> VectorIterator<'a> {
    pub fn new(v: &'a Vector<'a>) -> Self {
        Self { v, next_i: 0 }
    }
}
impl<'a> Iterator for VectorIterator<'a> {
    type Item = Scalar<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let curr_i = self.next_i;
        self.next_i += 1;

        if curr_i == self.v.len() {
            return None;
        }

        Some(self.v.get(curr_i))
    }
}

use chrono::prelude::*;

impl<'a> std::fmt::Display for Vector<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::String(v) => write!(f, "{:?}", v),
            Self::Float(v) => write!(f, "{:?}", v),
            Self::Integer(v) => {
                for x in v.iter() {
                    let ts = NaiveDateTime::from_timestamp(*x / 1000 / 1000, 0);
                    write!(f, "{}, ", ts)?;
                }
                Ok(())
            }
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
    pub fn value(&self, row_id: usize) -> Option<Scalar<'_>> {
        match self {
            Column::String(c) => {
                if row_id >= self.num_rows() {
                    return None;
                }

                match c.value(row_id) {
                    Some(v) => Some(Scalar::String(v)),
                    None => None,
                }
            }
            Column::Float(c) => {
                if row_id >= self.num_rows() {
                    return None;
                }

                let v = c.value(row_id);
                if let Some(v) = v {
                    return Some(Scalar::Float(v));
                }
                None
            }
            Column::Integer(c) => {
                if row_id >= self.num_rows() {
                    return None;
                }

                let v = c.value(row_id);
                if let Some(v) = v {
                    return Some(Scalar::Integer(v));
                }
                None
            }
        }
    }

    /// Materialise all of the decoded values matching the provided logical
    /// row ids.
    pub fn values(&self, row_ids: &[usize]) -> Vector<'_> {
        match self {
            Column::String(c) => {
                if row_ids.is_empty() {
                    return Vector::String(vec![]);
                }

                Vector::String(c.values(row_ids))
            }
            Column::Float(c) => {
                if row_ids.is_empty() {
                    return Vector::Float(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.values(row_ids);
                log::debug!("time getting decoded values for float {:?}", now.elapsed());

                Vector::Float(v)
            }
            Column::Integer(c) => {
                if row_ids.is_empty() {
                    return Vector::Integer(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.values(row_ids);
                log::debug!("time getting decoded values for int {:?}", now.elapsed());
                Vector::Integer(v)
            }
        }
    }

    /// Materialise all of the decoded values matching the provided logical
    /// row ids within the bitmap
    pub fn values_bitmap(&self, row_ids: &croaring::Bitmap) -> Vector<'_> {
        match self {
            Column::String(c) => {
                if row_ids.is_empty() {
                    return Vector::String(vec![]);
                }

                let row_id_vec = row_ids
                    .to_vec()
                    .iter()
                    .map(|v| *v as usize)
                    .collect::<Vec<_>>();
                Vector::String(c.values(&row_id_vec))
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
                Vector::Float(c.values(&row_id_vec))
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
                Vector::Integer(c.values(&row_id_vec))
            }
        }
    }

    /// Materialise all of the encoded values matching the provided logical
    /// row ids.
    pub fn encoded_values_bitmap(&self, row_ids: &croaring::Bitmap) -> Vector<'_> {
        let now = std::time::Instant::now();
        let row_ids_vec = row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        log::debug!("time unpacking bitmap {:?}", now.elapsed());

        match self {
            Column::String(c) => {
                if row_ids.is_empty() {
                    return Vector::Integer(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.encoded_values(&row_ids_vec);
                log::debug!("time getting encoded values {:?}", now.elapsed());
                Vector::EncodedString(v)
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
    pub fn encoded_values(&self, row_ids: &[usize]) -> Vector<'_> {
        match self {
            Column::String(c) => {
                if row_ids.is_empty() {
                    return Vector::Integer(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.encoded_values(&row_ids);
                log::debug!("time getting encoded values {:?}", now.elapsed());

                log::debug!("dictionary {:?}", c.data.dictionary());
                Vector::EncodedString(v)
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
    pub fn all_encoded_values(&self) -> Vector<'_> {
        match self {
            Column::String(c) => {
                let now = std::time::Instant::now();
                let v = c.all_encoded_values();
                log::debug!("time getting all encoded values {:?}", now.elapsed());

                log::debug!("dictionary {:?}", c.data.dictionary());
                Vector::EncodedString(v)
            }
            Column::Float(c) => Vector::Float(c.all_encoded_values()),
            Column::Integer(c) => Vector::Integer(c.all_encoded_values()),
        }
    }

    /// Given an encoded value for a row, materialise and return the decoded
    /// version.
    ///
    /// This currently just supports decoding integer scalars back into dictionary
    /// strings.
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
    pub fn rows(&self, row_ids: &croaring::Bitmap) -> Vector<'_> {
        let now = std::time::Instant::now();
        let row_ids_vec = row_ids
            .to_vec()
            .iter()
            .map(|v| *v as usize)
            .collect::<Vec<_>>();
        log::debug!("time unpacking bitmap {:?}", now.elapsed());

        assert!(
            row_ids_vec.len() == 1 || row_ids_vec[row_ids_vec.len() - 1] > row_ids_vec[0],
            "got last row_id={:?} and first row_id={:?}",
            row_ids_vec[row_ids_vec.len() - 1],
            row_ids_vec[0]
        );
        match self {
            Column::String(c) => Vector::String(c.values(&row_ids_vec)),
            Column::Float(c) => Vector::Float(c.values(&row_ids_vec)),
            Column::Integer(c) => Vector::Integer(c.values(&row_ids_vec)),
        }
    }

    pub fn maybe_contains(&self, value: &Scalar<'_>) -> bool {
        match self {
            Column::String(c) => {
                if let Scalar::String(v) = value {
                    c.meta.maybe_contains_value(v.to_string())
                } else {
                    panic!("invalid value");
                }
            }
            Column::Float(c) => {
                if let Scalar::Float(v) = value {
                    c.meta.maybe_contains_value(*v)
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Scalar::Integer(v) = value {
                    c.meta.maybe_contains_value(*v)
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
        }
    }

    /// returns true if the column cannot contain
    pub fn max_less_than(&self, value: &Scalar<'_>) -> bool {
        match self {
            Column::String(c) => {
                if let Scalar::String(v) = value {
                    if let Some(range) = c.meta.range() {
                        range.1 < v.to_string()
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
            Column::Float(c) => {
                if let Scalar::Float(v) = value {
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
                if let Scalar::Integer(v) = value {
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

    // TODO(edd): consolodate with max_less_than... Should just be single cmp function
    pub fn min_greater_than(&self, value: &Scalar<'_>) -> bool {
        match self {
            Column::String(c) => {
                if let Scalar::String(v) = value {
                    if let Some(range) = c.meta.range() {
                        range.0 > v.to_string()
                    } else {
                        false
                    }
                } else {
                    panic!("invalid value");
                }
            }
            Column::Float(c) => {
                if let Scalar::Float(v) = value {
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
                if let Scalar::Integer(v) = value {
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
    pub fn min(&self) -> Option<Scalar<'_>> {
        match self {
            Column::String(c) => match c.meta.range() {
                Some(range) => Some(Scalar::String(&range.0)),
                None => None,
            },
            Column::Float(c) => match c.meta.range() {
                Some(range) => Some(Scalar::Float(range.0)),
                None => None,
            },
            Column::Integer(c) => match c.meta.range() {
                Some(range) => Some(Scalar::Integer(range.0)),
                None => None,
            },
        }
    }

    /// Returns the maximum value contained within this column.
    // FIXME(edd): Support NULL integers and floats
    pub fn max(&self) -> Option<Scalar<'_>> {
        match self {
            Column::String(c) => match c.meta.range() {
                Some(range) => Some(Scalar::String(&range.1)),
                None => None,
            },
            Column::Float(c) => match c.meta.range() {
                Some(range) => Some(Scalar::Float(range.1)),
                None => None,
            },
            Column::Integer(c) => match c.meta.range() {
                Some(range) => Some(Scalar::Integer(range.1)),
                None => None,
            },
        }
    }

    pub fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> Option<Scalar<'_>> {
        match self {
            Column::String(_) => unimplemented!("not implemented"),
            Column::Float(c) => match c.sum_by_ids(row_ids) {
                Some(sum) => Some(Scalar::Float(sum)),
                None => None,
            },
            Column::Integer(_) => unimplemented!("not implemented"),
        }
    }

    pub fn aggregate_by_id_range(
        &self,
        agg_type: &AggregateType,
        from_row_id: usize,
        to_row_id: usize,
    ) -> Aggregate<'_> {
        match self {
            Column::String(_) => unimplemented!("not implemented"),
            Column::Float(c) => match agg_type {
                AggregateType::Count => {
                    Aggregate::Count(c.count_by_id_range(from_row_id, to_row_id) as u64)
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
    pub fn row_ids_eq(&self, value: Option<&Scalar<'_>>) -> Option<croaring::Bitmap> {
        let value = match value {
            Some(v) => v,
            None => return None,
        };

        if !self.maybe_contains(value) {
            return None;
        }
        self.row_ids(value, std::cmp::Ordering::Equal)
    }

    pub fn row_ids_gt(&self, value: &Scalar<'_>) -> Option<croaring::Bitmap> {
        if self.max_less_than(value) {
            return None;
        }
        self.row_ids(value, std::cmp::Ordering::Greater)
    }

    pub fn row_ids_lt(&self, value: &Scalar<'_>) -> Option<croaring::Bitmap> {
        if self.min_greater_than(value) {
            return None;
        }
        self.row_ids(value, std::cmp::Ordering::Less)
    }

    // allows you to do:
    //      WHERE time >= 0 AND time < 100
    //
    // or
    //
    //      WHERE counter >= 102.2 AND counter < 2929.32
    pub fn row_ids_gte_lt(&self, low: &Scalar<'_>, high: &Scalar<'_>) -> Option<croaring::Bitmap> {
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

                if let (Scalar::Float(low), Scalar::Float(high)) = (low, high) {
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

                if let (Scalar::Integer(low), Scalar::Integer(high)) = (low, high) {
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
    fn row_ids(&self, value: &Scalar<'_>, order: std::cmp::Ordering) -> Option<croaring::Bitmap> {
        match self {
            Column::String(c) => {
                if order != std::cmp::Ordering::Equal {
                    unimplemented!("> < not supported on strings yet");
                }

                if let Scalar::String(v) = value {
                    Some(c.data.row_ids_eq_roaring(Some(v.to_string())))
                } else {
                    panic!("invalid value");
                }
            }
            Column::Float(c) => {
                if let Scalar::Float(v) = value {
                    Some(c.data.row_ids_single_cmp_roaring(v, order))
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Scalar::Integer(v) = value {
                    Some(c.data.row_ids_single_cmp_roaring(v, order))
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
        }
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Column::String(c) => {
                write!(f, "{}", c)?;
            }
            Column::Float(c) => {
                write!(f, "{}", c)?;
            }
            Column::Integer(c) => {
                write!(f, "{}", c)?;
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
    ) -> Aggregate<'_> {
        Column::aggregate_by_id_range(&self, agg_type, from_row_id, to_row_id)
    }
}

// impl From<&[f64]> for Column {
//     fn from(values: &[f64]) -> Self {
//         Self::Float(Float::from(values))
//     }
// }

// impl From<&[i64]> for Column {
//     fn from(values: &[i64]) -> Self {
//         Self::Integer(Integer::from(values))
//     }
// }

#[derive(Debug, Default)]
pub struct String {
    meta: metadata::Metadata<std::string::String>,

    // TODO(edd): this would probably have multiple possible encodings
    data: encoding::DictionaryRLE,
}

impl String {
    pub fn with_dictionary(
        dictionary: std::collections::BTreeSet<Option<std::string::String>>,
    ) -> Self {
        let mut c = Self::default();
        c.data = encoding::DictionaryRLE::with_dictionary(dictionary);
        c
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

    pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<i64> {
        self.data.encoded_values(row_ids)
    }

    pub fn all_encoded_values(&self) -> Vec<i64> {
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

//     pub fn sum_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> f64 {
//         self.data.sum_by_id_range(from_row_id, to_row_id)
//     }

//     pub fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> usize {
//         self.data.count_by_id_range(from_row_id, to_row_id)
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
// impl From<arrow::array::PrimitiveArray<arrow::datatypes::Float64Type>> for Float {
//     fn from(arr: arrow::array::PrimitiveArray<arrow::datatypes::Float64Type>) -> Self {
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
    T: Clone + std::cmp::PartialOrd + std::fmt::Debug,
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

    pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<Option<T>> {
        self.data.encoded_values(row_ids)
    }

    pub fn all_encoded_values(&self) -> Vec<Option<T>> {
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

    pub fn count_by_id_range(&self, from_row_id: usize, to_row_id: usize) -> usize {
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
            match self.range {
                Some(range) => {
                    if v < range.0 {
                        range.0 = v;
                    }

                    if v > range.1 {
                        range.1 = v;
                    }
                }
                None => {
                    self.range = Some((v, v));
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
            match self.range {
                Some(range) => range.0 <= v && v <= range.1,
                None => false,
            }
        }

        pub fn range(&self) -> &Option<(T, T)> {
            &self.range
        }

        pub fn size(&self) -> usize {
            // size of types for num_rows and range
            let base_size = size_of::<usize>() + (2 * size_of::<Option<String>>());

            //
            //  TODO: figure out a way to specify that T must be able to describe its runtime size.
            //
            // match &self.range {
            //     (None, None) => base_size,
            //     (Some(min), None) => base_size + min.len(),
            //     (None, Some(max)) => base_size + max.len(),
            //     (Some(min), Some(max)) => base_size + min.len() + max.len(),
            // }
            base_size
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

    //     pub fn add_repeated(&mut self, s: Option<String>, additional: usize) {
    //         self.num_rows += additional;

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
    //         let base_size = size_of::<usize>() + (2 * size_of::<Option<String>>());
    //         match &self.range {
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
    //         log::debug!(
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
