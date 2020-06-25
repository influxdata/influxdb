//! This module contains code to pack values into a format suitable
//! for feeding to the parquet writer. It is destined for replacement
//! by an Apache Arrow based implementation

// Note the maintainability of this code is not likely high (it came
// from the copy pasta factory) but the plan is to replace it
// soon... We'll see how long that actually takes...
use parquet::data_type::ByteArray;
use std::default::Default;

// NOTE: See https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
// for an explanation of nesting levels

#[derive(Debug)]
pub enum Packers {
    Float(Packer<f64>),
    Integer(Packer<i64>),
    String(Packer<ByteArray>),
    Boolean(Packer<bool>),
}

macro_rules! typed_packer_accessors {
    ($(($name:ident, $name_mut:ident, $type:ty, $variant:ident),)*) => {
        $(
            pub fn $name(&self) -> &Packer<$type> {
                if let Self::$variant(p) = self {
                    p
                } else {
                    panic!(concat!("packer is not a ", stringify!($variant)));
                }
            }

            pub fn $name_mut(&mut self) -> &mut Packer<$type> {
                if let Self::$variant(p) = self {
                    p
                } else {
                    panic!(concat!("packer is not a ", stringify!($variant)));
                }
            }
        )*
    };
}

impl Packers {
    /// Create a String Packers with repeated values.
    pub fn from_elem_str(v: &str, n: usize) -> Self {
        Self::String(Packer::from(vec![ByteArray::from(v); n]))
    }

    /// Reserves the minimum capacity for exactly additional more elements to
    /// be inserted into the Packer<T>` without reallocation.
    pub fn reserve_exact(&mut self, additional: usize) {
        match self {
            Self::Float(p) => p.reserve_exact(additional),
            Self::Integer(p) => p.reserve_exact(additional),
            Self::String(p) => p.reserve_exact(additional),
            Self::Boolean(p) => p.reserve_exact(additional),
        }
    }

    /// See description on `Packer::num_rows`
    pub fn num_rows(&self) -> usize {
        match self {
            Self::Float(p) => p.num_rows(),
            Self::Integer(p) => p.num_rows(),
            Self::String(p) => p.num_rows(),
            Self::Boolean(p) => p.num_rows(),
        }
    }

    pub fn push_none(&mut self) {
        match self {
            Self::Float(p) => p.push_option(None),
            Self::Integer(p) => p.push_option(None),
            Self::String(p) => p.push_option(None),
            Self::Boolean(p) => p.push_option(None),
        }
    }

    /// Determines if the value for `row` is null is null.
    ///
    /// If there is no row then `is_null` returns `true`.
    pub fn is_null(&self, row: usize) -> bool {
        match self {
            Self::Float(p) => p.is_null(row),
            Self::Integer(p) => p.is_null(row),
            Self::String(p) => p.is_null(row),
            Self::Boolean(p) => p.is_null(row),
        }
    }

    // Implementations of all the accessors for the variants of `Packers`.
    typed_packer_accessors! {
        (f64_packer, f64_packer_mut, f64, Float),
        (i64_packer, i64_packer_mut, i64, Integer),
        (str_packer, str_packer_mut, ByteArray, String),
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
        Self::String(Packer::from(v))
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

impl std::convert::From<delorean_table_schema::DataType> for Packers {
    fn from(t: delorean_table_schema::DataType) -> Self {
        match t {
            delorean_table_schema::DataType::Float => Self::Float(Packer::<f64>::new()),
            delorean_table_schema::DataType::Integer => Self::Integer(Packer::<i64>::new()),
            delorean_table_schema::DataType::String => Self::String(Packer::<ByteArray>::new()),
            delorean_table_schema::DataType::Boolean => Self::Boolean(Packer::<bool>::new()),
            delorean_table_schema::DataType::Timestamp => Self::Integer(Packer::<i64>::new()),
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
        Self::String(Packer::from(as_byte_array))
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

#[derive(Debug, Default)]
pub struct Packer<T: Default> {
    values: Vec<T>,
    def_levels: Vec<i16>,
    rep_levels: Vec<i16>,
}

impl<T: Default> Packer<T> {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    /// Create a new packer with the specified capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
        }
    }

    /// Reserves the minimum capacity for exactly additional more elements to
    /// be inserted to the `Packer<T>` without reallocation.
    pub fn reserve_exact(&mut self, additional: usize) {
        self.values.reserve_exact(additional);
        self.def_levels.reserve_exact(additional);
        self.rep_levels.reserve_exact(additional);
    }

    /// Returns the number of logical rows represented in this
    /// packer. This will be different than self.values.len() when
    /// NULLs are present because there are no values in self.values
    /// stored for NULL
    pub fn num_rows(&self) -> usize {
        self.def_levels.len()
    }

    /// Get the value of logical row at `index`. This may be different
    /// than the index in self.values.len() when NULLs are present
    /// because there are no values in self.values stored for NULL
    pub fn get(&self, index: usize) -> Option<&T> {
        if self.def_levels[index] == 0 {
            None
        } else {
            let mut values_idx: usize = 0;
            for i in 0..index {
                let def_level = self.def_levels[i];
                if def_level != 0 && def_level != 1 {
                    panic!("unexpected def level. Expected 0 or 1, found {}", i);
                }
                values_idx += def_level as usize;
            }
            Some(&self.values[values_idx])
        }
    }

    // TODO(edd): I don't like these getters. They're only needed so we can
    // write the data into a parquet writer. We should have a method on Packer
    // that accepts some implementation of a trait that a parquet writer satisfies
    // and then pass the data through in here.
    pub fn values(&self) -> &[T] {
        &self.values
    }

    pub fn def_levels(&self) -> &[i16] {
        &self.def_levels
    }

    pub fn rep_levels(&self) -> &[i16] {
        &self.rep_levels
    }

    pub fn push_option(&mut self, value: Option<T>) {
        match value {
            Some(v) => self.push(v),
            None => {
                // NB: No value stored at def level == 0
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }

    pub fn push(&mut self, value: T) {
        self.values.push(value);
        self.def_levels.push(1);
        self.rep_levels.push(1);
    }

    /// Return true if the row for index is null. Returns true if there is no
    /// row for index.
    pub fn is_null(&self, index: usize) -> bool {
        self.def_levels.get(index).map_or(true, |&x| x == 0)
    }
}

// Convert `Vec<T>`, e.g., `Vec<f64>` into the appropriate `Packer<T>` value,
// e.g., `Packer<f64>`.
impl<T: Default> std::convert::From<Vec<T>> for Packer<T> {
    fn from(v: Vec<T>) -> Self {
        Self {
            def_levels: vec![1; v.len()],
            rep_levels: vec![1; v.len()],
            values: v,
        }
    }
}

// Convert `Vec<Option<T>>`, e.g., `Vec<Option<f64>>` into the appropriate
// `Packer<T>` value, e.g., `Packer<f64>`.
impl<T: Default> std::convert::From<Vec<Option<T>>> for Packer<T> {
    fn from(values: Vec<Option<T>>) -> Self {
        let mut packer = Self::new();
        for v in values {
            packer.push_option(v);
        }
        packer
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn with_capacity() {
        let packer: Packer<bool> = Packer::with_capacity(42);
        assert_eq!(packer.values.capacity(), 42);
        assert_eq!(packer.def_levels.capacity(), 42);
        assert_eq!(packer.rep_levels.capacity(), 42);
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
        let mut packers: Vec<Packers> = Vec::new();
        packers.push(Packers::Float(Packer::new()));
        packers.push(Packers::Integer(Packer::new()));
        packers.push(Packers::Boolean(Packer::new()));

        packers.get_mut(0).unwrap().f64_packer_mut().push(22.033);
    }

    #[test]
    fn packers_null_encoding() {
        let mut packer: Packer<ByteArray> = Packer::new();
        packer.push(ByteArray::from("foo"));
        packer.push_option(None);
        packer.push(ByteArray::from("bar"));

        assert_eq!(packer.num_rows(), 3);
        // Note there are only two values, even though there are 3 rows
        assert_eq!(
            packer.values,
            vec![ByteArray::from("foo"), ByteArray::from("bar")]
        );
        assert_eq!(packer.def_levels, vec![1, 0, 1]);
        assert_eq!(packer.rep_levels, vec![1, 1, 1]);
    }
}
