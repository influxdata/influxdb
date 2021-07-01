//! This module contains code to pack values into a format suitable
//! for feeding to the parquet writer. It is destined for replacement
//! by an Apache Arrow based implementation

// Note the maintainability of this code is not likely high (it came
// from the copy pasta factory) but the plan is to replace it
// soon... We'll see how long that actually takes...
use core::iter::Iterator;
use std::iter;
use std::slice::Chunks;

use internal_types::schema::{InfluxColumnType, InfluxFieldType};
use parquet::data_type::ByteArray;
use std::default::Default;

// NOTE: See https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
// for an explanation of nesting levels

#[derive(Debug, PartialEq)]
pub enum Packers {
    Float(Packer<f64>),
    Integer(Packer<i64>),
    UInteger(Packer<u64>),
    Bytes(Packer<ByteArray>),
    String(Packer<String>),
    Boolean(Packer<bool>),
}

macro_rules! typed_packer_accessors {
    ($(($name:ident, $name_mut:ident, $type:ty, $variant:ident),)*) => {
        $(
            pub fn $name(&self) -> &Packer<$type> {
                if let Self::$variant(p) = self {
                    p
                } else {
                    panic!(concat!("packer is not a ", stringify!($variant), " is {:?}"), self);
                }
            }

            pub fn $name_mut(&mut self) -> &mut Packer<$type> {
                if let Self::$variant(p) = self {
                    p
                } else {
                    panic!(concat!("packer is not a ", stringify!($variant), " is {:?}"), self);
                }
            }
        )*
    };
}

impl<'a> Packers {
    pub fn chunk_values(&self, chunk_size: usize) -> PackerChunker<'_> {
        match self {
            Self::Float(p) => PackerChunker::Float(p.values.chunks(chunk_size)),
            Self::Integer(p) => PackerChunker::Integer(p.values.chunks(chunk_size)),
            Self::UInteger(p) => PackerChunker::UInteger(p.values.chunks(chunk_size)),
            Self::Bytes(p) => PackerChunker::Bytes(p.values.chunks(chunk_size)),
            Self::String(p) => PackerChunker::String(p.values.chunks(chunk_size)),
            Self::Boolean(p) => PackerChunker::Boolean(p.values.chunks(chunk_size)),
        }
    }

    /// Create a String Packers with repeated values.
    pub fn from_elem_str(v: &str, n: usize) -> Self {
        Self::Bytes(Packer::from(vec![ByteArray::from(v); n]))
    }

    /// Reserves the minimum capacity for exactly additional more elements to
    /// be inserted into the Packer<T>` without reallocation.
    pub fn reserve_exact(&mut self, additional: usize) {
        match self {
            Self::Float(p) => p.reserve_exact(additional),
            Self::Integer(p) => p.reserve_exact(additional),
            Self::UInteger(p) => p.reserve_exact(additional),
            Self::Bytes(p) => p.reserve_exact(additional),
            Self::String(p) => p.reserve_exact(additional),
            Self::Boolean(p) => p.reserve_exact(additional),
        }
    }

    pub fn push_none(&mut self) {
        match self {
            Self::Float(p) => p.push_option(None),
            Self::Integer(p) => p.push_option(None),
            Self::UInteger(p) => p.push_option(None),
            Self::Bytes(p) => p.push_option(None),
            Self::String(p) => p.push_option(None),
            Self::Boolean(p) => p.push_option(None),
        }
    }

    /// swap two elements within a Packers variant
    pub fn swap(&mut self, a: usize, b: usize) {
        match self {
            Self::Float(p) => p.swap(a, b),
            Self::Integer(p) => p.swap(a, b),
            Self::UInteger(p) => p.swap(a, b),
            Self::Bytes(p) => p.swap(a, b),
            Self::String(p) => p.swap(a, b),
            Self::Boolean(p) => p.swap(a, b),
        }
    }

    /// See description on `Packer::num_rows`
    pub fn num_rows(&self) -> usize {
        match self {
            Self::Float(p) => p.num_rows(),
            Self::Integer(p) => p.num_rows(),
            Self::UInteger(p) => p.num_rows(),
            Self::Bytes(p) => p.num_rows(),
            Self::String(p) => p.num_rows(),
            Self::Boolean(p) => p.num_rows(),
        }
    }

    /// Determines if the value for `row` is null is null.
    ///
    /// If there is no row then `is_null` returns `true`.
    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Float(p) => p.is_null(row),
            Self::Integer(p) => p.is_null(row),
            Self::UInteger(p) => p.is_null(row),
            Self::Bytes(p) => p.is_null(row),
            Self::String(p) => p.is_null(row),
            Self::Boolean(p) => p.is_null(row),
        }
    }

    // Implementations of all the accessors for the variants of `Packers`.
    typed_packer_accessors! {
        (f64_packer, f64_packer_mut, f64, Float),
        (i64_packer, i64_packer_mut, i64, Integer),
        (u64_packer, u64_packer_mut, u64, UInteger),
        (bytes_packer, bytes_packer_mut, ByteArray, Bytes),
        (str_packer, str_packer_mut, String, String),
        (bool_packer, bool_packer_mut, bool, Boolean),
    }
}

impl std::convert::From<Vec<i64>> for Packers {
    fn from(v: Vec<i64>) -> Self {
        Self::Integer(Packer::from(v))
    }
}

impl std::convert::From<Vec<f64>> for Packers {
    fn from(v: Vec<f64>) -> Self {
        Self::Float(Packer::from(v))
    }
}

impl std::convert::From<Vec<ByteArray>> for Packers {
    fn from(v: Vec<ByteArray>) -> Self {
        Self::Bytes(Packer::from(v))
    }
}

impl std::convert::From<Vec<bool>> for Packers {
    fn from(v: Vec<bool>) -> Self {
        Self::Boolean(Packer::from(v))
    }
}

impl std::convert::From<Vec<Option<i64>>> for Packers {
    fn from(v: Vec<Option<i64>>) -> Self {
        Self::Integer(Packer::from(v))
    }
}

impl std::convert::From<Vec<Option<f64>>> for Packers {
    fn from(v: Vec<Option<f64>>) -> Self {
        Self::Float(Packer::from(v))
    }
}

impl std::convert::From<Vec<Option<bool>>> for Packers {
    fn from(v: Vec<Option<bool>>) -> Self {
        Self::Boolean(Packer::from(v))
    }
}

impl std::convert::From<Vec<Option<u64>>> for Packers {
    fn from(values: Vec<Option<u64>>) -> Self {
        // TODO(edd): convert this with an iterator?
        let mut as_i64: Vec<Option<i64>> = Vec::with_capacity(values.len());
        for v in values {
            match v {
                Some(v) => as_i64.push(Some(v as i64)),
                None => as_i64.push(None),
            }
        }
        Self::Integer(Packer::from(as_i64))
    }
}

impl std::convert::From<Vec<Option<String>>> for Packers {
    fn from(v: Vec<Option<String>>) -> Self {
        Self::String(Packer::from(v.as_slice()))
    }
}

impl std::convert::From<InfluxColumnType> for Packers {
    fn from(t: InfluxColumnType) -> Self {
        match t {
            InfluxColumnType::IOx(_) => todo!(),
            InfluxColumnType::Tag => Self::Bytes(Packer::<ByteArray>::new()),
            InfluxColumnType::Field(InfluxFieldType::Float) => Self::Float(Packer::<f64>::new()),
            InfluxColumnType::Field(InfluxFieldType::Integer) => {
                Self::Integer(Packer::<i64>::new())
            }
            InfluxColumnType::Field(InfluxFieldType::UInteger) => {
                unimplemented!();
            }
            InfluxColumnType::Field(InfluxFieldType::String) => {
                Self::Bytes(Packer::<ByteArray>::new())
            }
            InfluxColumnType::Field(InfluxFieldType::Boolean) => {
                Self::Boolean(Packer::<bool>::new())
            }
            InfluxColumnType::Timestamp => Self::Integer(Packer::<i64>::new()),
        }
    }
}

impl std::convert::From<influxdb_tsm::BlockType> for Packers {
    fn from(t: influxdb_tsm::BlockType) -> Self {
        match t {
            influxdb_tsm::BlockType::Float => Self::Float(Packer::<f64>::new()),
            influxdb_tsm::BlockType::Integer => Self::Integer(Packer::<i64>::new()),
            influxdb_tsm::BlockType::Str => Self::Bytes(Packer::<ByteArray>::new()),
            influxdb_tsm::BlockType::Bool => Self::Boolean(Packer::<bool>::new()),
            influxdb_tsm::BlockType::Unsigned => Self::Integer(Packer::<i64>::new()),
        }
    }
}

impl std::convert::From<Vec<Option<Vec<u8>>>> for Packers {
    fn from(values: Vec<Option<Vec<u8>>>) -> Self {
        // TODO(edd): convert this with an iterator?
        let mut as_byte_array: Vec<Option<ByteArray>> = Vec::with_capacity(values.len());
        for v in values {
            match v {
                Some(v) => as_byte_array.push(Some(ByteArray::from(v))),
                None => as_byte_array.push(None),
            }
        }
        Self::Bytes(Packer::from(as_byte_array))
    }
}

/// PackerChunker represents chunkable Packer variants.
#[derive(Debug)]
pub enum PackerChunker<'a> {
    Float(Chunks<'a, Option<f64>>),
    Integer(Chunks<'a, Option<i64>>),
    UInteger(Chunks<'a, Option<u64>>),
    Bytes(Chunks<'a, Option<ByteArray>>),
    String(Chunks<'a, Option<String>>),
    Boolean(Chunks<'a, Option<bool>>),
}

#[derive(Debug, Default, PartialEq)]
pub struct Packer<T>
where
    T: Default + Clone,
{
    values: Vec<Option<T>>,
}

impl<T> Packer<T>
where
    T: Default + Clone + std::fmt::Debug,
{
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Create a new packer with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
        }
    }

    /// Reserves the minimum capacity for exactly additional more elements to
    /// be inserted to the `Packer<T>` without reallocation.
    pub fn reserve_exact(&mut self, additional: usize) {
        self.values.reserve_exact(additional);
    }

    /// Returns the number of logical rows represented in this
    /// packer. This will be different than self.values.len() when
    /// NULLs are present because there are no values in self.values
    /// stored for NULL
    pub fn num_rows(&self) -> usize {
        self.values.len()
    }

    /// Get the value of logical row at `index`.
    pub fn get(&self, index: usize) -> Option<&T> {
        self.values[index].as_ref()
    }

    pub fn iter(&self) -> PackerIterator<'_, T> {
        PackerIterator::new(&self)
    }

    // TODO(edd): I don't like these getters. They're only needed so we can
    // write the data into a parquet writer. We should have a method on Packer
    // that accepts some implementation of a trait that a parquet writer satisfies
    // and then pass the data through in here.
    pub fn values(&self) -> &[Option<T>] {
        &self.values
    }

    /// Returns an iterator that emits `chunk_size` values from the Packer until
    /// all values are returned.
    pub fn chunk_values(&self, chunk_size: usize) -> std::slice::Chunks<'_, Option<T>> {
        self.values.chunks(chunk_size)
    }

    /// Returns a binary vector indicating which indexes have null values.
    pub fn def_levels(&self) -> Vec<i16> {
        self.values
            .iter()
            .map(|v| if v.is_some() { 1 } else { 0 })
            .collect()
    }

    /// returns all of the non-null values in the Packer
    pub fn some_values(&self) -> Vec<T> {
        self.values.iter().filter_map(|x| x.clone()).collect()
    }

    pub fn push_option(&mut self, value: Option<T>) {
        self.values.push(value);
    }

    pub fn push(&mut self, value: T) {
        self.push_option(Some(value));
    }

    pub fn extend_from_packer(&mut self, other: &Self) {
        self.values.extend_from_slice(&other.values);
    }

    pub fn extend_from_slice(&mut self, other: &[T]) {
        self.values.extend(other.iter().cloned().map(Some));
    }

    pub fn extend_from_option_slice(&mut self, other: &[Option<T>]) {
        self.values.extend_from_slice(other);
    }

    /// Populate the Packer with additional more values of T.
    pub fn fill_with(&mut self, value: T, additional: usize) {
        self.values
            .extend(iter::repeat(Some(value)).take(additional));
    }

    pub fn fill_with_null(&mut self, additional: usize) {
        self.values.extend(iter::repeat(None).take(additional));
    }

    pub fn swap(&mut self, a: usize, b: usize) {
        self.values.swap(a, b);
    }

    /// Return true if the logic value at index is null. Returns true if there
    /// is no row for index.
    pub fn is_null(&self, index: usize) -> bool {
        self.values.get(index).map_or(true, Option::is_none)
    }
}

#[derive(Debug)]
pub struct PackerIterator<'a, T>
where
    T: Default + Clone,
{
    packer: &'a Packer<T>,
    iter_i: usize,
}

impl<'a, T> PackerIterator<'a, T>
where
    T: Default + Clone,
{
    fn new(packer: &'a Packer<T>) -> Self {
        PackerIterator { packer, iter_i: 0 }
    }
}

impl<'a, T> Iterator for PackerIterator<'a, T>
where
    T: Default + Clone + std::fmt::Debug,
{
    type Item = Option<&'a T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.iter_i == self.packer.num_rows() {
            return None;
        }

        let curr_iter_i = self.iter_i;
        self.iter_i += 1;
        Some(self.packer.values[curr_iter_i].as_ref())
    }
}

// Convert `Vec<T>`, e.g., `Vec<f64>` into the appropriate `Packer<T>` value,
// e.g., `Packer<f64>`.
impl<T> std::convert::From<Vec<T>> for Packer<T>
where
    T: Default + Clone,
{
    fn from(v: Vec<T>) -> Self {
        Self {
            values: v.into_iter().map(Some).collect(),
        }
    }
}

// Convert `Vec<Option<T>>`, e.g., `Vec<Option<f64>>` into the appropriate
// `Packer<T>` value, e.g., `Packer<f64>`.
impl<T> std::convert::From<Vec<Option<T>>> for Packer<T>
where
    T: Default + Clone + std::fmt::Debug,
{
    fn from(values: Vec<Option<T>>) -> Self {
        let mut packer = Self::new();
        for v in values {
            packer.push_option(v);
        }
        packer
    }
}

// Convert `&[<Option<T>]`, e.g., `&[Option<f64>]` into the appropriate
// `Packer<T>` value, e.g., `Packer<f64>`.
impl<T> std::convert::From<&[Option<T>]> for Packer<T>
where
    T: Default + Clone + std::fmt::Debug,
{
    fn from(values: &[Option<T>]) -> Self {
        let mut packer = Self::new();
        for v in values {
            packer.push_option(v.clone());
        }
        packer
    }
}

#[cfg(test)]
mod test {
    use super::*;

    // helper to extract a Vec<T> from a Packer<T>, panicking if there are any
    // None values in the Packer.
    fn must_materialise_some_values<T>(p: &Packer<T>) -> Vec<T>
    where
        T: Default + Clone,
    {
        p.values
            .iter()
            .cloned()
            .map(|x| x.expect("got None value"))
            .collect()
    }

    #[test]
    fn with_capacity() {
        let packer: Packer<bool> = Packer::with_capacity(42);
        assert_eq!(packer.values.capacity(), 42);
    }

    #[test]
    fn extend_from_slice() {
        let mut packer: Packer<i64> = Packer::new();
        packer.push(100);
        packer.push(22);

        packer.extend_from_slice(&[2, 3, 4]);

        assert_eq!(must_materialise_some_values(&packer), &[100, 22, 2, 3, 4]);
        assert_eq!(packer.def_levels(), &[1; 5]);
    }

    #[test]
    fn extend_from_packer() {
        let mut packer_a: Packer<i64> = Packer::new();
        packer_a.push(100);
        packer_a.push(22);

        let mut packer_b = Packer::new();
        packer_b.push(3);

        packer_a.extend_from_packer(&packer_b);
        assert_eq!(must_materialise_some_values(&packer_a), &[100, 22, 3]);
        assert_eq!(packer_a.def_levels(), &[1; 3]);
    }

    #[test]
    fn pad_with_null() {
        let mut packer: Packer<i64> = Packer::new();
        packer.push(100);
        packer.push(22);

        packer.fill_with_null(3);

        assert_eq!(packer.values, &[Some(100), Some(22), None, None, None]);
        assert_eq!(packer.def_levels(), &[1, 1, 0, 0, 0]);
    }

    #[test]
    fn is_null() {
        let mut packer: Packer<f64> = Packer::new();
        packer.push(22.3);
        packer.push_option(Some(100.3));
        packer.push_option(None);
        packer.push(33.3);

        assert_eq!(packer.is_null(0), false);
        assert_eq!(packer.is_null(1), false);
        assert_eq!(packer.is_null(2), true);
        assert_eq!(packer.is_null(3), false);
        assert_eq!(packer.is_null(4), true); // out of bounds
    }

    #[test]
    fn packers_create() {
        let mut packers = vec![
            Packers::Float(Packer::new()),
            Packers::Integer(Packer::new()),
            Packers::UInteger(Packer::new()),
            Packers::Boolean(Packer::new()),
        ];

        packers.get_mut(0).unwrap().f64_packer_mut().push(22.033);
    }

    #[test]
    fn packers_null_encoding() {
        let mut packer: Packer<ByteArray> = Packer::new();
        packer.push(ByteArray::from("foo"));
        packer.push_option(None);
        packer.push(ByteArray::from("bar"));

        assert_eq!(packer.num_rows(), 3);
        assert_eq!(
            packer.values,
            vec![
                Some(ByteArray::from("foo")),
                None,
                Some(ByteArray::from("bar"))
            ]
        );
        assert_eq!(packer.def_levels(), vec![1, 0, 1]);
    }

    #[test]
    fn packers_iter() {
        let mut packer = Packer::new();
        packer.push(100_u64);
        packer.push_option(None);
        packer.push(200);
        packer.push_option(None);
        packer.push_option(None);

        let values: Vec<_> = packer.iter().collect();

        assert_eq!(values[0], Some(&100));
        assert_eq!(values[1], None);
        assert_eq!(values[2], Some(&200));
        assert_eq!(values[3], None);
        assert_eq!(values[4], None);
    }
}
