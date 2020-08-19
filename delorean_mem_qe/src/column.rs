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
                    return Self::Float(v + other);
                } else {
                    panic!("invalid");
                };
            }
            Self::Integer(v) => {
                if let Self::Integer(other) = _rhs {
                    return Self::Integer(v + other);
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
    Sum(Scalar<'a>),
}

impl<'a> Aggregate<'a> {
    pub fn update_with(&mut self, other: Scalar<'a>) {
        match self {
            Self::Count(v) => {
                *v = *v + 1;
            }
            Self::Sum(v) => {
                v.add(other);
            }
        }
    }
}

impl<'a> std::ops::Add<Scalar<'a>> for Aggregate<'a> {
    type Output = Aggregate<'a>;

    fn add(self, _rhs: Scalar<'a>) -> Self::Output {
        match self {
            Self::Count(c) => Self::Count(c + 1),
            Self::Sum(s) => Self::Sum(s + &_rhs),
        }
    }
}

impl<'a> std::ops::Add<&Aggregate<'a>> for Aggregate<'a> {
    type Output = Aggregate<'a>;

    fn add(self, _rhs: &Aggregate<'a>) -> Self::Output {
        match self {
            Self::Count(c) => {
                if let Self::Count(other) = _rhs {
                    return Self::Count(c + other);
                } else {
                    panic!("invalid");
                };
            }
            Self::Sum(s) => {
                if let Self::Sum(other) = _rhs {
                    return Self::Sum(s + other);
                } else {
                    panic!("invalid");
                };
            }
        }
    }
}

// impl<'a> std::ops::Add<&Scalar<'a>> for Aggregate<'a> {
//     type Output = Aggregate<'a>;

//     fn add(self, _rhs: &Scalar<'a>) -> Self::Output {
//         match _rhs {
//             Scalar::String(v) => {}
//             Scalar::Float(v) => {}
//             Scalar::Integer(v) => {}
//         }
//         // match self {
//         //     Self::Count(c) => {
//         //         match
//         //         if let Scalar::Count(other) = _rhs {
//         //             return Self::Count(c + other);
//         //         } else {
//         //             panic!("invalid");
//         //         };
//         //     }
//         //     Self::Sum(s) => {
//         //         if let Self::Sum(other) = _rhs {
//         //             return Self::Sum(s + other);
//         //         } else {
//         //             panic!("invalid");
//         //         };
//         //     }
//         // }
//     }
// }

pub enum Vector<'a> {
    String(Vec<&'a Option<std::string::String>>),
    Float(Vec<f64>),
    Integer(Vec<i64>),
}

impl<'a> Vector<'a> {
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
    Float(Float),
    Integer(Integer),
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

    /// Materialise all of the decoded values matching the provided logical
    /// row ids.
    pub fn value(&self, row_id: usize) -> Option<Scalar> {
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
                Some(Scalar::Float(c.value(row_id)))
            }
            Column::Integer(c) => {
                if row_id >= self.num_rows() {
                    return None;
                }
                Some(Scalar::Integer(c.value(row_id)))
            }
        }
    }

    /// Materialise all of the decoded values matching the provided logical
    /// row ids.
    pub fn values(&self, row_ids: &[usize]) -> Vector {
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
    pub fn values_bitmap(&self, row_ids: &croaring::Bitmap) -> Vector {
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
    pub fn encoded_values_bitmap(&self, row_ids: &croaring::Bitmap) -> Vector {
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
                Vector::Integer(v)
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
                    return Vector::Integer(vec![]);
                }

                let now = std::time::Instant::now();
                let v = c.encoded_values(&row_ids);
                log::debug!("time getting encoded values {:?}", now.elapsed());

                log::debug!("dictionary {:?}", c.data.dictionary());
                Vector::Integer(v)
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
    pub fn rows(&self, row_ids: &croaring::Bitmap) -> Vector {
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

    /// materialise all rows including and after row_id
    pub fn scan_from(&self, _row_id: usize) -> Option<Vector> {
        unimplemented!("todo");
        // if row_id >= self.num_rows() {
        //     println!(
        //         "asking for {:?} but only got {:?} rows",
        //         row_id,
        //         self.num_rows()
        //     );
        //     return None;
        // }

        // println!(
        //     "asking for {:?} with a column having {:?} rows",
        //     row_id,
        //     self.num_rows()
        // );
        // match self {
        //     Column::String(c) => Some(Vector::String(c.scan_from(row_id))),
        //     Column::Float(c) => Some(Vector::Float(c.scan_from(row_id))),
        //     Column::Integer(c) => Some(Vector::Integer(c.scan_from(row_id))),
        // }
    }

    /// Given the provided row_id scans the column until a non-null value found
    /// or the column is exhausted.
    pub fn scan_from_until_some(&self, row_id: usize) -> Option<Scalar> {
        match self {
            Column::String(c) => {
                if row_id >= self.num_rows() {
                    return None;
                }

                match c.scan_from_until_some(row_id) {
                    Some(v) => Some(Scalar::String(v)),
                    None => None,
                }
            }
            Column::Float(c) => {
                if row_id >= self.num_rows() {
                    return None;
                }
                match c.scan_from_until_some(row_id) {
                    Some(v) => Some(Scalar::Float(v)),
                    None => None,
                }
            }
            Column::Integer(c) => {
                if row_id >= self.num_rows() {
                    return None;
                }
                match c.scan_from_until_some(row_id) {
                    Some(v) => Some(Scalar::Integer(v)),
                    None => None,
                }
            }
        }
    }

    pub fn maybe_contains(&self, value: Option<&Scalar>) -> bool {
        match self {
            Column::String(c) => match value {
                Some(scalar) => {
                    if let Scalar::String(v) = scalar {
                        c.meta.maybe_contains_value(Some(v.to_string()))
                    } else {
                        panic!("invalid value");
                    }
                }
                None => c.meta.maybe_contains_value(None),
            },
            Column::Float(c) => {
                if let Some(Scalar::Float(v)) = value {
                    c.meta.maybe_contains_value(v.to_owned())
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Some(Scalar::Integer(v)) = value {
                    c.meta.maybe_contains_value(v.to_owned())
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
        }
    }

    /// returns true if the column cannot contain
    pub fn max_less_than(&self, value: Option<&Scalar>) -> bool {
        match self {
            Column::String(c) => match value {
                Some(scalar) => {
                    if let Scalar::String(v) = scalar {
                        c.meta.range().1 < Some(&v.to_string())
                    } else {
                        panic!("invalid value");
                    }
                }
                None => c.meta.range().1 < None,
            },
            Column::Float(c) => {
                if let Some(Scalar::Float(v)) = value {
                    c.meta.range().1 < *v
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Some(Scalar::Integer(v)) = value {
                    c.meta.range().1 < *v
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
        }
    }

    pub fn min_greater_than(&self, value: Option<&Scalar>) -> bool {
        match self {
            Column::String(c) => match value {
                Some(scalar) => {
                    if let Scalar::String(v) = scalar {
                        c.meta.range().0 > Some(&v.to_string())
                    } else {
                        panic!("invalid value");
                    }
                }
                None => c.meta.range().0 > None,
            },
            Column::Float(c) => {
                if let Some(Scalar::Float(v)) = value {
                    c.meta.range().0 > *v
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Some(Scalar::Integer(v)) = value {
                    c.meta.range().0 > *v
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
        }
    }

    /// Returns the minimum value contained within this column.
    // FIXME(edd): Support NULL integers and floats
    pub fn min(&self) -> Option<Scalar> {
        match self {
            Column::String(c) => {
                if let Some(min) = c.meta.range().0 {
                    return Some(Scalar::String(min));
                }
                None
            }
            Column::Float(c) => Some(Scalar::Float(c.meta.range().0)),
            Column::Integer(c) => Some(Scalar::Integer(c.meta.range().0)),
        }
    }

    /// Returns the maximum value contained within this column.
    // FIXME(edd): Support NULL integers and floats
    pub fn max(&self) -> Option<Scalar> {
        match self {
            Column::String(c) => {
                if let Some(max) = c.meta.range().1 {
                    return Some(Scalar::String(max));
                }
                None
            }
            Column::Float(c) => Some(Scalar::Float(c.meta.range().1)),
            Column::Integer(c) => Some(Scalar::Integer(c.meta.range().1)),
        }
    }

    pub fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> Option<Scalar> {
        match self {
            Column::String(_) => unimplemented!("not implemented"),
            Column::Float(c) => Some(Scalar::Float(c.sum_by_ids(row_ids))),
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
    pub fn row_ids_eq(&self, value: Option<&Scalar>) -> Option<croaring::Bitmap> {
        if !self.maybe_contains(value) {
            return None;
        }
        self.row_ids(value, std::cmp::Ordering::Equal)
    }

    pub fn row_ids_gt(&self, value: Option<&Scalar>) -> Option<croaring::Bitmap> {
        if self.max_less_than(value) {
            return None;
        }
        self.row_ids(value, std::cmp::Ordering::Greater)
    }

    pub fn row_ids_lt(&self, value: Option<&Scalar>) -> Option<croaring::Bitmap> {
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
    pub fn row_ids_gte_lt(&self, low: &Scalar, high: &Scalar) -> Option<croaring::Bitmap> {
        match self {
            Column::String(_c) => {
                unimplemented!("not implemented yet");
            }
            Column::Float(c) => {
                let (col_min, col_max) = c.meta.range();
                if let (Scalar::Float(low), Scalar::Float(high)) = (low, high) {
                    if *low <= col_min && *high > col_max {
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
                let (col_min, col_max) = c.meta.range();
                if let (Scalar::Integer(low), Scalar::Integer(high)) = (low, high) {
                    if *low <= col_min && *high > col_max {
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
    fn row_ids(
        &self,
        value: Option<&Scalar>,
        order: std::cmp::Ordering,
    ) -> Option<croaring::Bitmap> {
        match self {
            Column::String(c) => {
                if order != std::cmp::Ordering::Equal {
                    unimplemented!("> < not supported on strings yet");
                }
                match value {
                    Some(scalar) => {
                        if let Scalar::String(v) = scalar {
                            Some(c.data.row_ids_eq_roaring(Some(v.to_string())))
                        } else {
                            panic!("invalid value");
                        }
                    }
                    None => Some(c.data.row_ids_eq_roaring(None)),
                }
            }
            Column::Float(c) => {
                if let Some(Scalar::Float(v)) = value {
                    Some(c.data.row_ids_single_cmp_roaring(v, order))
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Some(Scalar::Integer(v)) = value {
                    Some(c.data.row_ids_single_cmp_roaring(v, order))
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
        }
    }
}

impl From<&[f64]> for Column {
    fn from(values: &[f64]) -> Self {
        Self::Float(Float::from(values))
    }
}

impl From<&[i64]> for Column {
    fn from(values: &[i64]) -> Self {
        Self::Integer(Integer::from(values))
    }
}

#[derive(Debug, Default)]
pub struct String {
    meta: metadata::Str,

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

    pub fn column_range(&self) -> (Option<&std::string::String>, Option<&std::string::String>) {
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

    /// Return the decoded value for an encoded ID.
    ///
    /// Panics if there is no decoded value for the provided id
    pub fn decode_id(&self, encoded_id: i64) -> Option<std::string::String> {
        self.data.decode_id(encoded_id as usize)
    }

    pub fn scan_from(&self, row_id: usize) -> Vec<&Option<std::string::String>> {
        self.data.scan_from(row_id)
    }

    pub fn scan_from_until_some(&self, _row_id: usize) -> Option<&std::string::String> {
        unreachable!("don't need this");
        // self.data.scan_from_until_some(row_id)
    }

    // TODO(edd) shouldn't let roaring stuff leak out...
    pub fn group_row_ids(&self) -> &std::collections::BTreeMap<u32, croaring::Bitmap> {
        self.data.group_row_ids()
    }
}

#[derive(Debug, Default)]
pub struct Float {
    meta: metadata::F64,

    // TODO(edd): compression of float columns
    data: encoding::PlainFixed<f64>,
}

impl Float {
    pub fn column_range(&self) -> (f64, f64) {
        self.meta.range()
    }

    pub fn size(&self) -> usize {
        self.meta.size() + self.data.size()
    }

    pub fn value(&self, row_id: usize) -> f64 {
        self.data.value(row_id)
    }

    pub fn values(&self, row_ids: &[usize]) -> Vec<f64> {
        self.data.values(row_ids)
    }

    pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<f64> {
        self.data.encoded_values(row_ids)
    }

    pub fn scan_from(&self, row_id: usize) -> &[f64] {
        self.data.scan_from(row_id)
    }

    pub fn scan_from_until_some(&self, row_id: usize) -> Option<f64> {
        self.data.scan_from_until_some(row_id)
    }

    pub fn sum_by_ids(&self, row_ids: &mut croaring::Bitmap) -> f64 {
        self.data.sum_by_ids(row_ids)
    }
}

impl From<&[f64]> for Float {
    fn from(values: &[f64]) -> Self {
        let len = values.len();
        let mut min = std::f64::MAX;
        let mut max = std::f64::MIN;

        // calculate min/max for meta data
        for v in values {
            min = min.min(*v);
            max = max.max(*v);
        }

        Self {
            meta: metadata::F64::new((min, max), len),
            data: encoding::PlainFixed::from(values),
        }
    }
}

#[derive(Debug, Default)]
pub struct Integer {
    meta: metadata::I64,

    // TODO(edd): compression of integers
    data: encoding::PlainFixed<i64>,
}

impl Integer {
    pub fn column_range(&self) -> (i64, i64) {
        self.meta.range()
    }

    pub fn size(&self) -> usize {
        self.meta.size() + self.data.size()
    }

    pub fn value(&self, row_id: usize) -> i64 {
        self.data.value(row_id)
    }

    pub fn values(&self, row_ids: &[usize]) -> Vec<i64> {
        self.data.values(row_ids)
    }

    pub fn encoded_values(&self, row_ids: &[usize]) -> Vec<i64> {
        self.data.encoded_values(row_ids)
    }

    pub fn scan_from(&self, row_id: usize) -> &[i64] {
        self.data.scan_from(row_id)
    }

    pub fn scan_from_until_some(&self, row_id: usize) -> Option<i64> {
        self.data.scan_from_until_some(row_id)
    }

    /// Find the first logical row that contains this value.
    pub fn row_id_eq_value(&self, v: i64) -> Option<usize> {
        if !self.meta.maybe_contains_value(v) {
            return None;
        }
        self.data.row_id_eq_value(v)
    }

    /// Find the first logical row that contains a value >= v
    pub fn row_id_ge_value(&self, v: i64) -> Option<usize> {
        if self.meta.max() < v {
            return None;
        }
        self.data.row_id_ge_value(v)
    }
}

impl From<&[i64]> for Integer {
    fn from(values: &[i64]) -> Self {
        let len = values.len();
        let mut min = std::i64::MAX;
        let mut max = std::i64::MIN;

        // calculate min/max for meta data
        for v in values {
            min = min.min(*v);
            max = max.max(*v);
        }

        Self {
            meta: metadata::I64::new((min, max), len),
            data: encoding::PlainFixed::from(values),
        }
    }
}

pub mod metadata {
    #[derive(Debug, Default)]
    pub struct Str {
        range: (Option<String>, Option<String>),
        num_rows: usize,
        // sparse_index: BTreeMap<String, usize>,
    }

    impl Str {
        pub fn add(&mut self, s: Option<String>) {
            self.num_rows += 1;

            if s < self.range.0 {
                self.range.0 = s.clone();
            }

            if s > self.range.1 {
                self.range.1 = s;
            }
        }

        pub fn add_repeated(&mut self, s: Option<String>, additional: usize) {
            self.num_rows += additional;

            if s < self.range.0 {
                self.range.0 = s.clone();
            }

            if s > self.range.1 {
                self.range.1 = s;
            }
        }

        pub fn num_rows(&self) -> usize {
            self.num_rows
        }

        pub fn maybe_contains_value(&self, v: Option<String>) -> bool {
            self.range.0 <= v && v <= self.range.1
        }

        pub fn range(&self) -> (Option<&String>, Option<&String>) {
            (self.range.0.as_ref(), self.range.1.as_ref())
        }

        pub fn size(&self) -> usize {
            // TODO!!!!
            0 //self.range.0.len() + self.range.1.len() + std::mem::size_of::<usize>()
        }
    }

    #[derive(Debug, Default)]
    pub struct F64 {
        range: (f64, f64),
        num_rows: usize,
    }

    impl F64 {
        pub fn new(range: (f64, f64), rows: usize) -> Self {
            Self {
                range,
                num_rows: rows,
            }
        }

        pub fn maybe_contains_value(&self, v: f64) -> bool {
            let res = self.range.0 <= v && v <= self.range.1;
            log::debug!(
                "column with ({:?}) maybe contain {:?} -- {:?}",
                self.range,
                v,
                res
            );
            res
        }

        pub fn num_rows(&self) -> usize {
            self.num_rows
        }

        pub fn range(&self) -> (f64, f64) {
            self.range
        }

        pub fn size(&self) -> usize {
            std::mem::size_of::<(f64, f64)>() + std::mem::size_of::<usize>()
        }
    }

    #[derive(Debug, Default)]
    pub struct I64 {
        range: (i64, i64),
        num_rows: usize,
    }

    impl I64 {
        pub fn new(range: (i64, i64), rows: usize) -> Self {
            Self {
                range,
                num_rows: rows,
            }
        }

        pub fn maybe_contains_value(&self, v: i64) -> bool {
            self.range.0 <= v && v <= self.range.1
        }

        pub fn max(&self) -> i64 {
            self.range.1
        }

        pub fn num_rows(&self) -> usize {
            self.num_rows
        }

        pub fn range(&self) -> (i64, i64) {
            self.range
        }

        pub fn size(&self) -> usize {
            std::mem::size_of::<(i64, i64)>() + std::mem::size_of::<usize>()
        }
    }
}
