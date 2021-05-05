use std::{convert::TryFrom, fmt::Formatter};
use std::{mem::size_of, sync::Arc};

use crate::{AggregateType, LogicalDataType};

#[derive(Clone, PartialEq, Debug)]
/// A type that holds aggregates where each variant encodes the underlying data
/// type and aggregate type for a vector of data. An `AggregateVec` can be
/// updated on a value-by-value basis and new values can be appended.
///
/// The type is structured this way to improve the performance of aggregations
/// in the read buffer by reducing the number of matches (branches) needed per
/// row.
pub enum AggregateVec {
    Count(Vec<Option<u64>>),

    SumI64(Vec<Option<i64>>),
    SumU64(Vec<Option<u64>>),
    SumF64(Vec<Option<f64>>),

    MinU64(Vec<Option<u64>>),
    MinI64(Vec<Option<i64>>),
    MinF64(Vec<Option<f64>>),
    MinString(Vec<Option<String>>),
    MinBytes(Vec<Option<Vec<u8>>>),
    MinBool(Vec<Option<bool>>),

    MaxU64(Vec<Option<u64>>),
    MaxI64(Vec<Option<i64>>),
    MaxF64(Vec<Option<f64>>),
    MaxString(Vec<Option<String>>),
    MaxBytes(Vec<Option<Vec<u8>>>),
    MaxBool(Vec<Option<bool>>),

    FirstU64((Vec<Option<u64>>, Vec<Option<i64>>)),
    FirstI64((Vec<Option<i64>>, Vec<Option<i64>>)),
    FirstF64((Vec<Option<f64>>, Vec<Option<i64>>)),
    FirstString((Vec<Option<String>>, Vec<Option<i64>>)),
    FirstBytes((Vec<Option<Vec<u8>>>, Vec<Option<i64>>)),
    FirstBool((Vec<Option<bool>>, Vec<Option<i64>>)),

    LastU64((Vec<Option<u64>>, Vec<Option<i64>>)),
    LastI64((Vec<Option<i64>>, Vec<Option<i64>>)),
    LastF64((Vec<Option<f64>>, Vec<Option<i64>>)),
    LastString((Vec<Option<String>>, Vec<Option<i64>>)),
    LastBytes((Vec<Option<Vec<u8>>>, Vec<Option<i64>>)),
    LastBool((Vec<Option<bool>>, Vec<Option<i64>>)),
}

impl AggregateVec {
    pub fn len(&self) -> usize {
        match self {
            Self::Count(arr) => arr.len(),
            Self::SumI64(arr) => arr.len(),
            Self::SumU64(arr) => arr.len(),
            Self::SumF64(arr) => arr.len(),
            Self::MinU64(arr) => arr.len(),
            Self::MinI64(arr) => arr.len(),
            Self::MinF64(arr) => arr.len(),
            Self::MinString(arr) => arr.len(),
            Self::MinBytes(arr) => arr.len(),
            Self::MinBool(arr) => arr.len(),
            Self::MaxU64(arr) => arr.len(),
            Self::MaxI64(arr) => arr.len(),
            Self::MaxF64(arr) => arr.len(),
            Self::MaxString(arr) => arr.len(),
            Self::MaxBytes(arr) => arr.len(),
            Self::MaxBool(arr) => arr.len(),
            Self::FirstU64((arr, _)) => arr.len(),
            Self::FirstI64((arr, _)) => arr.len(),
            Self::FirstF64((arr, _)) => arr.len(),
            Self::FirstString((arr, _)) => arr.len(),
            Self::FirstBytes((arr, _)) => arr.len(),
            Self::FirstBool((arr, _)) => arr.len(),
            Self::LastU64((arr, _)) => arr.len(),
            Self::LastI64((arr, _)) => arr.len(),
            Self::LastF64((arr, _)) => arr.len(),
            Self::LastString((arr, _)) => arr.len(),
            Self::LastBytes((arr, _)) => arr.len(),
            Self::LastBool((arr, _)) => arr.len(),
        }
    }

    /// Returns the value specified by `offset`.
    pub fn value(&self, offset: usize) -> Value<'_> {
        match &self {
            Self::Count(arr) => Value::from(arr[offset]),
            Self::SumI64(arr) => Value::from(arr[offset]),
            Self::SumU64(arr) => Value::from(arr[offset]),
            Self::SumF64(arr) => Value::from(arr[offset]),
            Self::MinU64(arr) => Value::from(arr[offset]),
            Self::MinI64(arr) => Value::from(arr[offset]),
            Self::MinF64(arr) => Value::from(arr[offset]),
            Self::MinString(arr) => Value::from(arr[offset].as_deref()),
            Self::MinBytes(arr) => Value::from(arr[offset].as_deref()),
            Self::MinBool(arr) => Value::from(arr[offset]),
            Self::MaxU64(arr) => Value::from(arr[offset]),
            Self::MaxI64(arr) => Value::from(arr[offset]),
            Self::MaxF64(arr) => Value::from(arr[offset]),
            Self::MaxString(arr) => Value::from(arr[offset].as_deref()),
            Self::MaxBytes(arr) => Value::from(arr[offset].as_deref()),
            Self::MaxBool(arr) => Value::from(arr[offset]),
            _ => unimplemented!("first/last not yet implemented"),
        }
    }

    /// Updates with a new value located in the provided input column help in
    /// `Values`.
    ///
    /// Panics if the type of `Value` does not satisfy the aggregate type.
    pub fn update(&mut self, values: &Values<'_>, row_id: usize, offset: usize) {
        if values.is_null(row_id) {
            return;
        }

        match self {
            Self::Count(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                *arr[offset].get_or_insert(0) += 1;
            }
            Self::SumI64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v += values.value_i64(row_id),
                    None => arr[offset] = Some(values.value_i64(row_id)),
                }
            }
            Self::SumU64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v += values.value_u64(row_id),
                    None => arr[offset] = Some(values.value_u64(row_id)),
                }
            }
            Self::SumF64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v += values.value_f64(row_id),
                    None => arr[offset] = Some(values.value_f64(row_id)),
                }
            }
            Self::MinU64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).min(values.value_u64(row_id)),
                    None => arr[offset] = Some(values.value_u64(row_id)),
                }
            }
            Self::MinI64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).min(values.value_i64(row_id)),
                    None => arr[offset] = Some(values.value_i64(row_id)),
                }
            }
            Self::MinF64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).min(values.value_f64(row_id)),
                    None => arr[offset] = Some(values.value_f64(row_id)),
                }
            }
            Self::MinString(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => {
                        let other = values.value_str(row_id);
                        if other < v.as_str() {
                            *v = other.to_owned();
                        }
                    }
                    None => arr[offset] = Some(values.value_str(row_id).to_owned()),
                }
            }
            Self::MinBytes(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => {
                        let other = values.value_bytes(row_id);
                        if other < v.as_slice() {
                            *v = other.to_owned();
                        }
                    }
                    None => arr[offset] = Some(values.value_bytes(row_id).to_owned()),
                }
            }
            Self::MinBool(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).min(values.value_bool(row_id)),
                    None => arr[offset] = Some(values.value_bool(row_id)),
                }
            }
            Self::MaxU64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).max(values.value_u64(row_id)),
                    None => arr[offset] = Some(values.value_u64(row_id)),
                }
            }
            Self::MaxI64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).max(values.value_i64(row_id)),
                    None => arr[offset] = Some(values.value_i64(row_id)),
                }
            }
            Self::MaxF64(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).max(values.value_f64(row_id)),
                    None => arr[offset] = Some(values.value_f64(row_id)),
                }
            }
            Self::MaxString(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => {
                        let other = values.value_str(row_id);
                        if other > v.as_str() {
                            *v = other.to_owned();
                        }
                    }
                    None => arr[offset] = Some(values.value_str(row_id).to_owned()),
                }
            }
            Self::MaxBytes(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => {
                        let other = values.value_bytes(row_id);
                        if other > v.as_slice() {
                            *v = other.to_owned();
                        }
                    }
                    None => arr[offset] = Some(values.value_bytes(row_id).to_owned()),
                }
            }
            Self::MaxBool(arr) => {
                if offset >= arr.len() {
                    arr.resize(offset + 1, None);
                }

                match &mut arr[offset] {
                    Some(v) => *v = (*v).max(values.value_bool(row_id)),
                    None => arr[offset] = Some(values.value_bool(row_id)),
                }
            }
            // TODO - implement first/last
            _ => unimplemented!("aggregate update not implemented"),
        }
    }

    /// Appends the provided value to the end of the aggregate vector.
    /// Panics if the type of `Value` does not satisfy the aggregate type.
    ///
    /// Note: updating pushed first/last variants is not currently a supported
    /// operation.
    pub fn push(&mut self, value: Value<'_>) {
        match self {
            Self::Count(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.u64()));
                }
            }
            Self::SumI64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.i64()));
                }
            }
            Self::SumU64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.u64()));
                }
            }
            Self::SumF64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.f64()));
                }
            }
            Self::MinU64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.u64()));
                }
            }
            Self::MinI64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.i64()));
                }
            }
            Self::MinF64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.f64()));
                }
            }
            Self::MinString(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.str().to_owned()));
                }
            }
            Self::MinBytes(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bytes().to_owned()));
                }
            }
            Self::MinBool(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bool()));
                }
            }
            Self::MaxU64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.u64()));
                }
            }
            Self::MaxI64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.i64()));
                }
            }
            Self::MaxF64(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.f64()));
                }
            }
            Self::MaxString(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.str().to_owned()));
                }
            }
            Self::MaxBytes(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bytes().to_owned()));
                }
            }
            Self::MaxBool(arr) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bool()));
                }
            }
            Self::FirstU64((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.u64()));
                }
            }
            Self::FirstI64((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.i64()));
                }
            }
            Self::FirstF64((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.f64()));
                }
            }
            Self::FirstString((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.str().to_owned()));
                }
            }
            Self::FirstBytes((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bytes().to_owned()));
                }
            }
            Self::FirstBool((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bool()));
                }
            }
            Self::LastU64((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.u64()));
                }
            }
            Self::LastI64((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.i64()));
                }
            }
            Self::LastF64((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.f64()));
                }
            }
            Self::LastString((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.str().to_owned()));
                }
            }
            Self::LastBytes((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bytes().to_owned()));
                }
            }
            Self::LastBool((arr, _)) => {
                if value.is_null() {
                    arr.push(None);
                } else {
                    arr.push(Some(value.bool()));
                }
            }
        }
    }

    /// Writes a textual representation of the value specified by `offset` to
    /// the provided formatter.
    pub fn write_value(&self, offset: usize, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::SumI64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::SumU64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::SumF64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MinU64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MinI64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MinF64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MinString(arr) => match &arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MinBytes(arr) => match &arr[offset] {
                Some(v) => write!(f, "{:?}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MinBool(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MaxU64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MaxI64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MaxF64(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MaxString(arr) => match &arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MaxBytes(arr) => match &arr[offset] {
                Some(v) => write!(f, "{:?}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::MaxBool(arr) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::FirstU64((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::FirstI64((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::FirstF64((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::FirstString((arr, _)) => match &arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::FirstBytes((arr, _)) => match &arr[offset] {
                Some(v) => write!(f, "{:?}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::FirstBool((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::LastU64((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::LastI64((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::LastF64((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::LastString((arr, _)) => match &arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::LastBytes((arr, _)) => match &arr[offset] {
                Some(v) => write!(f, "{:?}", v)?,
                None => write!(f, "NULL")?,
            },
            Self::LastBool((arr, _)) => match arr[offset] {
                Some(v) => write!(f, "{}", v)?,
                None => write!(f, "NULL")?,
            },
        }
        Ok(())
    }

    // Consumes self and returns the inner `Vec<Option<i64>>`.
    pub fn take_as_i64(self) -> Vec<Option<i64>> {
        match self {
            Self::SumI64(arr) => arr,
            Self::MinI64(arr) => arr,
            Self::MaxI64(arr) => arr,
            _ => panic!("cannot convert {} to Vec<Option<i64>>", self),
        }
    }

    // Consumes self and returns the inner `Vec<Option<u64>>`.
    pub fn take_as_u64(self) -> Vec<Option<u64>> {
        match self {
            Self::Count(arr) => arr,
            Self::SumU64(arr) => arr,
            Self::MinU64(arr) => arr,
            Self::MaxU64(arr) => arr,
            _ => panic!("cannot convert {} to Vec<Option<u64>>", self),
        }
    }

    // Consumes self and returns the inner `Vec<Option<f64>>`.
    pub fn take_as_f64(self) -> Vec<Option<f64>> {
        match self {
            Self::SumF64(arr) => arr,
            Self::MinF64(arr) => arr,
            Self::MaxF64(arr) => arr,
            _ => panic!("cannot convert {} to Vec<Option<f64>>", self),
        }
    }

    // Consumes self and returns the inner `Vec<Option<String>>`.
    pub fn take_as_str(self) -> Vec<Option<String>> {
        match self {
            Self::MinString(arr) => arr,
            Self::MaxString(arr) => arr,
            _ => panic!("cannot convert {} to Vec<Option<&str>>", self),
        }
    }

    // Consumes self and returns the inner `Vec<Option<Vec<u8>>>`.
    pub fn take_as_bytes(self) -> Vec<Option<Vec<u8>>> {
        match self {
            Self::MinBytes(arr) => arr,
            Self::MaxBytes(arr) => arr,
            _ => panic!("cannot convert {} to Vec<Option<&[u8]>>", self),
        }
    }

    // Consumes self and returns the inner `Vec<Option<bool>>`.
    pub fn take_as_bool(self) -> Vec<Option<bool>> {
        match self {
            Self::MinBool(arr) => arr,
            Self::MaxBool(arr) => arr,
            _ => panic!("cannot convert {} to Vec<u64>", self),
        }
    }

    /// Extends the `AggregateVec` with the provided `Option<i64>` iterator.
    pub fn extend_with_i64(&mut self, itr: impl Iterator<Item = Option<i64>>) {
        match self {
            Self::SumI64(arr) => {
                arr.extend(itr);
            }
            Self::MinI64(arr) => {
                arr.extend(itr);
            }
            Self::MaxI64(arr) => {
                arr.extend(itr);
            }
            _ => panic!("unsupported iterator"),
        }
    }

    /// Extends the `AggregateVec` with the provided `Option<u64>` iterator.
    pub fn extend_with_u64(&mut self, itr: impl Iterator<Item = Option<u64>>) {
        match self {
            Self::Count(arr) => {
                arr.extend(itr);
            }
            Self::SumU64(arr) => {
                arr.extend(itr);
            }
            Self::MinU64(arr) => {
                arr.extend(itr);
            }
            Self::MaxU64(arr) => {
                arr.extend(itr);
            }
            _ => panic!("unsupported iterator"),
        }
    }

    /// Extends the `AggregateVec` with the provided `Option<f64>` iterator.
    pub fn extend_with_f64(&mut self, itr: impl Iterator<Item = Option<f64>>) {
        match self {
            Self::SumF64(arr) => {
                arr.extend(itr);
            }
            Self::MinF64(arr) => {
                arr.extend(itr);
            }
            Self::MaxF64(arr) => {
                arr.extend(itr);
            }
            _ => panic!("unsupported iterator"),
        }
    }

    /// Extends the `AggregateVec` with the provided `Option<String>` iterator.
    pub fn extend_with_str(&mut self, itr: impl Iterator<Item = Option<String>>) {
        match self {
            Self::MinString(arr) => {
                arr.extend(itr);
            }
            Self::MaxString(arr) => {
                arr.extend(itr);
            }
            _ => panic!("unsupported iterator"),
        }
    }

    /// Extends the `AggregateVec` with the provided `Option<Vec<u8>>` iterator.
    pub fn extend_with_bytes(&mut self, itr: impl Iterator<Item = Option<Vec<u8>>>) {
        match self {
            Self::MinBytes(arr) => {
                arr.extend(itr);
            }
            Self::MaxBytes(arr) => {
                arr.extend(itr);
            }
            _ => panic!("unsupported iterator"),
        }
    }

    /// Extends the `AggregateVec` with the provided `Option<bool>` iterator.
    pub fn extend_with_bool(&mut self, itr: impl Iterator<Item = Option<bool>>) {
        match self {
            Self::MinBool(arr) => {
                arr.extend(itr);
            }
            Self::MaxBool(arr) => {
                arr.extend(itr);
            }
            _ => panic!("unsupported iterator"),
        }
    }

    pub fn sort_with_permutation(&mut self, p: &permutation::Permutation) {
        match self {
            Self::Count(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::SumI64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::SumU64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::SumF64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MinU64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MinI64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MinF64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MinString(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MinBytes(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MinBool(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MaxU64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MaxI64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MaxF64(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MaxString(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MaxBytes(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::MaxBool(arr) => {
                *arr = p.apply_slice(arr.as_slice());
            }
            Self::FirstU64((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::FirstI64((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::FirstF64((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::FirstString((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::FirstBytes((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::FirstBool((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::LastU64((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::LastI64((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::LastF64((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::LastString((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::LastBytes((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
            Self::LastBool((arr, time)) => {
                *arr = p.apply_slice(arr.as_slice());
                *time = p.apply_slice(time.as_slice());
            }
        }
    }
}

impl std::fmt::Display for AggregateVec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Count(_) => write!(f, "Count"),
            Self::SumI64(_) => write!(f, "Sum<i64>"),
            Self::SumU64(_) => write!(f, "Sum<u64>"),
            Self::SumF64(_) => write!(f, "Sum<f64>"),
            Self::MinU64(_) => write!(f, "Min<u64>"),
            Self::MinI64(_) => write!(f, "Min<i64>"),
            Self::MinF64(_) => write!(f, "Min<f64>"),
            Self::MinString(_) => write!(f, "Min<String>"),
            Self::MinBytes(_) => write!(f, "Min<Vec<u8>>"),
            Self::MinBool(_) => write!(f, "Min<bool>"),
            Self::MaxU64(_) => write!(f, "Max<u64>"),
            Self::MaxI64(_) => write!(f, "Max<i64>"),
            Self::MaxF64(_) => write!(f, "Max<f64>"),
            Self::MaxString(_) => write!(f, "Max<String>"),
            Self::MaxBytes(_) => write!(f, "Max<Vec<u8>>"),
            Self::MaxBool(_) => write!(f, "Max<bool>"),
            Self::FirstU64(_) => write!(f, "First<u64>"),
            Self::FirstI64(_) => write!(f, "First<i64>"),
            Self::FirstF64(_) => write!(f, "First<f64>"),
            Self::FirstString(_) => write!(f, "First<String>"),
            Self::FirstBytes(_) => write!(f, "First<Vec<u8>>"),
            Self::FirstBool(_) => write!(f, "First<bool>"),
            Self::LastU64(_) => write!(f, "Last<u64>"),
            Self::LastI64(_) => write!(f, "Last<i64>"),
            Self::LastF64(_) => write!(f, "Last<f64>"),
            Self::LastString(_) => write!(f, "Last<String>"),
            Self::LastBytes(_) => write!(f, "Last<Vec<u8>>"),
            Self::LastBool(_) => write!(f, "Last<bool>"),
        }
    }
}

impl From<(&AggregateType, &LogicalDataType)> for AggregateVec {
    fn from(v: (&AggregateType, &LogicalDataType)) -> Self {
        match (v.0, v.1) {
            (AggregateType::Count, _) => Self::Count(vec![]),
            (AggregateType::First, LogicalDataType::Integer) => Self::FirstI64((vec![], vec![])),
            (AggregateType::First, LogicalDataType::Unsigned) => Self::FirstU64((vec![], vec![])),
            (AggregateType::First, LogicalDataType::Float) => Self::FirstF64((vec![], vec![])),
            (AggregateType::First, LogicalDataType::String) => Self::FirstString((vec![], vec![])),
            (AggregateType::First, LogicalDataType::Binary) => Self::FirstBytes((vec![], vec![])),
            (AggregateType::First, LogicalDataType::Boolean) => Self::FirstBool((vec![], vec![])),
            (AggregateType::Last, LogicalDataType::Integer) => Self::LastI64((vec![], vec![])),
            (AggregateType::Last, LogicalDataType::Unsigned) => Self::LastU64((vec![], vec![])),
            (AggregateType::Last, LogicalDataType::Float) => Self::LastF64((vec![], vec![])),
            (AggregateType::Last, LogicalDataType::String) => Self::LastString((vec![], vec![])),
            (AggregateType::Last, LogicalDataType::Binary) => Self::LastBytes((vec![], vec![])),
            (AggregateType::Last, LogicalDataType::Boolean) => Self::LastBool((vec![], vec![])),
            (AggregateType::Min, LogicalDataType::Integer) => Self::MinI64(vec![]),
            (AggregateType::Min, LogicalDataType::Unsigned) => Self::MinU64(vec![]),
            (AggregateType::Min, LogicalDataType::Float) => Self::MinF64(vec![]),
            (AggregateType::Min, LogicalDataType::String) => Self::MinString(vec![]),
            (AggregateType::Min, LogicalDataType::Binary) => Self::MinBytes(vec![]),
            (AggregateType::Min, LogicalDataType::Boolean) => Self::MinBool(vec![]),
            (AggregateType::Max, LogicalDataType::Integer) => Self::MaxI64(vec![]),
            (AggregateType::Max, LogicalDataType::Unsigned) => Self::MaxU64(vec![]),
            (AggregateType::Max, LogicalDataType::Float) => Self::MaxF64(vec![]),
            (AggregateType::Max, LogicalDataType::String) => Self::MaxString(vec![]),
            (AggregateType::Max, LogicalDataType::Binary) => Self::MaxBytes(vec![]),
            (AggregateType::Max, LogicalDataType::Boolean) => Self::MaxBool(vec![]),
            (AggregateType::Sum, LogicalDataType::Integer) => Self::SumI64(vec![]),
            (AggregateType::Sum, LogicalDataType::Unsigned) => Self::SumU64(vec![]),
            (AggregateType::Sum, LogicalDataType::Float) => Self::SumF64(vec![]),
            (AggregateType::Sum, _) => unreachable!("unsupported SUM aggregates"),
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

impl std::ops::AddAssign<&Self> for Scalar {
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

impl std::ops::Add for Scalar {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Null, Self::Null) => Self::Null,
            (Self::Null, Self::I64(_)) => other,
            (Self::Null, Self::U64(_)) => other,
            (Self::Null, Self::F64(_)) => other,
            (Self::I64(_), Self::Null) => self,
            (Self::I64(a), Self::I64(b)) => Self::I64(a + b),
            (Self::U64(_), Self::Null) => self,
            (Self::U64(a), Self::U64(b)) => Self::U64(a + b),
            (Self::F64(_), Self::Null) => self,
            (Self::F64(a), Self::F64(b)) => Self::F64(a + b),
            (a, b) => panic!("{:?} + {:?} is an unsupported operation", a, b),
        }
    }
}

impl std::fmt::Display for &Scalar {
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

impl std::fmt::Display for &OwnedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedValue::Null => write!(f, "NULL"),
            OwnedValue::String(s) => s.fmt(f),
            OwnedValue::ByteArray(s) => write!(f, "{}", String::from_utf8_lossy(s)),
            OwnedValue::Boolean(b) => b.fmt(f),
            OwnedValue::Scalar(s) => s.fmt(f),
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

impl<'a> Value<'a> {
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn scalar(&self) -> &Scalar {
        if let Self::Scalar(s) = self {
            return s;
        }
        panic!("cannot unwrap Value to Scalar");
    }

    pub fn i64(self) -> i64 {
        if let Self::Scalar(Scalar::I64(v)) = self {
            return v;
        }
        panic!("cannot unwrap Value to i64");
    }

    pub fn u64(self) -> u64 {
        if let Self::Scalar(Scalar::U64(v)) = self {
            return v;
        }
        panic!("cannot unwrap Value to u64");
    }

    pub fn f64(self) -> f64 {
        if let Self::Scalar(Scalar::F64(v)) = self {
            return v;
        }
        panic!("cannot unwrap Value to f64");
    }

    pub fn str(self) -> &'a str {
        if let Self::String(s) = self {
            return s;
        }
        panic!("cannot unwrap Value to String");
    }

    pub fn bytes(self) -> &'a [u8] {
        if let Self::ByteArray(s) = self {
            return s;
        }
        panic!("cannot unwrap Value to byte array");
    }

    pub fn bool(self) -> bool {
        if let Self::Boolean(b) = self {
            return b;
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

impl<'a> From<Option<&'a str>> for Value<'a> {
    fn from(v: Option<&'a str>) -> Self {
        match v {
            Some(s) => Self::String(s),
            None => Self::Null,
        }
    }
}

impl<'a> From<&'a [u8]> for Value<'a> {
    fn from(v: &'a [u8]) -> Self {
        Self::ByteArray(v)
    }
}

impl<'a> From<Option<&'a [u8]>> for Value<'a> {
    fn from(v: Option<&'a [u8]>) -> Self {
        match v {
            Some(s) => Self::ByteArray(s),
            None => Self::Null,
        }
    }
}

impl<'a> From<bool> for Value<'a> {
    fn from(v: bool) -> Self {
        Self::Boolean(v)
    }
}

impl<'a> From<Option<bool>> for Value<'a> {
    fn from(v: Option<bool>) -> Self {
        match v {
            Some(s) => Self::Boolean(s),
            None => Self::Null,
        }
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

impl std::ops::Add for Value<'_> {
    type Output = Self;

    fn add(self, other: Self) -> Self {
        match (self, other) {
            (Self::Scalar(a), Self::Scalar(b)) => Self::Scalar(a + b),
            _ => panic!("unsupported operation on Value"),
        }
    }
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

    pub fn is_null(&self, i: usize) -> bool {
        match &self {
            Self::String(c) => c[i].is_none(),
            Self::F64(_) => false,
            Self::I64(_) => false,
            Self::U64(_) => false,
            Self::Bool(c) => c[i].is_none(),
            Self::ByteArray(c) => c[i].is_none(),
            Self::I64N(c) => c[i].is_none(),
            Self::U64N(c) => c[i].is_none(),
            Self::F64N(c) => c[i].is_none(),
        }
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

    // Returns a value as an i64. Panics if not possible.
    fn value_i64(&self, i: usize) -> i64 {
        match &self {
            Values::I64(c) => c[i],
            Values::I64N(c) => c[i].unwrap(),
            _ => panic!("value cannot be returned as i64"),
        }
    }

    // Returns a value as an u64. Panics if not possible.
    fn value_u64(&self, i: usize) -> u64 {
        match &self {
            Values::U64(c) => c[i],
            Values::U64N(c) => c[i].unwrap(),
            _ => panic!("value cannot be returned as u64"),
        }
    }

    // Returns a value as an f64. Panics if not possible.
    fn value_f64(&self, i: usize) -> f64 {
        match &self {
            Values::F64(c) => c[i],
            Values::F64N(c) => c[i].unwrap(),
            _ => panic!("value cannot be returned as f64"),
        }
    }

    // Returns a value as a &str. Panics if not possible.
    fn value_str(&self, i: usize) -> &'a str {
        match &self {
            Values::String(c) => c[i].unwrap(),
            _ => panic!("value cannot be returned as &str"),
        }
    }

    // Returns a value as a binary array. Panics if not possible.
    fn value_bytes(&self, i: usize) -> &'a [u8] {
        match &self {
            Values::ByteArray(c) => c[i].unwrap(),
            _ => panic!("value cannot be returned as &str"),
        }
    }

    // Returns a value as a bool. Panics if not possible.
    fn value_bool(&self, i: usize) -> bool {
        match &self {
            Values::Bool(c) => c[i].unwrap(),
            _ => panic!("value cannot be returned as &str"),
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
    fn aggregate_vec_update() {
        // i64
        let values = Values::I64N(vec![Some(1), Some(2), Some(3), None, Some(-1), Some(2)]);

        let mut aggs = vec![
            AggregateVec::Count(vec![]),
            AggregateVec::SumI64(vec![]),
            AggregateVec::MinI64(vec![]),
            AggregateVec::MaxI64(vec![]),
        ];

        for i in 0..values.len() {
            for agg in &mut aggs {
                agg.update(&values, i, 0);
            }
        }

        assert_eq!(
            aggs,
            vec![
                AggregateVec::Count(vec![Some(5)]),
                AggregateVec::SumI64(vec![Some(7)]),
                AggregateVec::MinI64(vec![Some(-1)]),
                AggregateVec::MaxI64(vec![Some(3)]),
            ]
        );

        // u64
        let values = Values::U64N(vec![Some(1), Some(2), Some(3), None, Some(0), Some(2)]);

        let mut aggs = vec![
            AggregateVec::Count(vec![]),
            AggregateVec::SumU64(vec![]),
            AggregateVec::MinU64(vec![]),
            AggregateVec::MaxU64(vec![]),
        ];

        for i in 0..values.len() {
            for agg in &mut aggs {
                agg.update(&values, i, 0);
            }
        }

        assert_eq!(
            aggs,
            vec![
                AggregateVec::Count(vec![Some(5)]),
                AggregateVec::SumU64(vec![Some(8)]),
                AggregateVec::MinU64(vec![Some(0)]),
                AggregateVec::MaxU64(vec![Some(3)]),
            ]
        );

        // f64
        let values = Values::F64N(vec![
            Some(1.0),
            Some(2.0),
            Some(3.0),
            None,
            Some(0.0),
            Some(2.0),
        ]);

        let mut aggs = vec![
            AggregateVec::Count(vec![]),
            AggregateVec::SumF64(vec![]),
            AggregateVec::MinF64(vec![]),
            AggregateVec::MaxF64(vec![]),
        ];

        for i in 0..values.len() {
            for agg in &mut aggs {
                agg.update(&values, i, 0);
            }
        }

        assert_eq!(
            aggs,
            vec![
                AggregateVec::Count(vec![Some(5)]),
                AggregateVec::SumF64(vec![Some(8.0)]),
                AggregateVec::MinF64(vec![Some(0.0)]),
                AggregateVec::MaxF64(vec![Some(3.0)]),
            ]
        );

        // string
        let values = Values::String(vec![
            Some("Pop Song 89"),
            Some("Orange Crush"),
            Some("Stand"),
            None,
        ]);

        let mut aggs = vec![
            AggregateVec::Count(vec![]),
            AggregateVec::MinString(vec![]),
            AggregateVec::MaxString(vec![]),
        ];

        for i in 0..values.len() {
            for agg in &mut aggs {
                agg.update(&values, i, 0);
            }
        }

        assert_eq!(
            aggs,
            vec![
                AggregateVec::Count(vec![Some(3)]),
                AggregateVec::MinString(vec![Some("Orange Crush".to_owned())]),
                AggregateVec::MaxString(vec![Some("Stand".to_owned())]),
            ]
        );

        // bytes
        let arr = vec![
            "Pop Song 89".to_string(),
            "Orange Crush".to_string(),
            "Stand".to_string(),
        ];
        let values = Values::ByteArray(vec![
            Some(arr[0].as_bytes()),
            Some(arr[1].as_bytes()),
            Some(arr[2].as_bytes()),
            None,
        ]);

        let mut aggs = vec![
            AggregateVec::Count(vec![]),
            AggregateVec::MinBytes(vec![]),
            AggregateVec::MaxBytes(vec![]),
        ];

        for i in 0..values.len() {
            for agg in &mut aggs {
                agg.update(&values, i, 0);
            }
        }

        assert_eq!(
            aggs,
            vec![
                AggregateVec::Count(vec![Some(3)]),
                AggregateVec::MinBytes(vec![Some(arr[1].bytes().collect())]),
                AggregateVec::MaxBytes(vec![Some(arr[2].bytes().collect())]),
            ]
        );

        // bool
        let values = Values::Bool(vec![Some(true), None, Some(false)]);

        let mut aggs = vec![
            AggregateVec::Count(vec![]),
            AggregateVec::MinBool(vec![]),
            AggregateVec::MaxBool(vec![]),
        ];

        for i in 0..values.len() {
            for agg in &mut aggs {
                agg.update(&values, i, 0);
            }
        }

        assert_eq!(
            aggs,
            vec![
                AggregateVec::Count(vec![Some(2)]),
                AggregateVec::MinBool(vec![Some(false)]),
                AggregateVec::MaxBool(vec![Some(true)]),
            ]
        );
    }

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
