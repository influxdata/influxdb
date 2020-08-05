use std::convert::From;

use super::encoding;

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Scalar<'a> {
    String(&'a str),
    Float(f64),
    Integer(i64),
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

    // Returns the size of the segment in bytes.
    pub fn size(&self) -> usize {
        match self {
            Column::String(c) => c.size(),
            Column::Float(c) => c.size(),
            Column::Integer(c) => c.size(),
        }
    }

    pub fn min(&self) -> Scalar {
        match self {
            Column::String(c) => Scalar::String(c.meta.range().0),
            Column::Float(c) => Scalar::Float(c.meta.range().0),
            Column::Integer(c) => Scalar::Integer(c.meta.range().0),
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
    pub fn add(&mut self, s: &str) {
        self.meta.add(s);
        self.data.push(s);
    }

    pub fn add_additional(&mut self, s: &str, additional: u64) {
        self.meta.add(s);
        self.data.push_additional(s, additional);
    }

    pub fn column_range(&self) -> (&str, &str) {
        self.meta.range()
    }

    pub fn size(&self) -> usize {
        self.meta.size() + self.data.size()
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
        range: (String, String),
        num_rows: usize,
        // sparse_index: BTreeMap<String, usize>,
    }

    impl Str {
        pub fn add(&mut self, s: &str) {
            self.num_rows += 1;

            if self.range.0.as_str() > s {
                self.range.0 = s.to_owned();
            }

            if self.range.1.as_str() < s {
                self.range.1 = s.to_owned();
            }
        }

        pub fn num_rows(&self) -> usize {
            self.num_rows
        }

        pub fn range(&self) -> (&str, &str) {
            (&self.range.0, &self.range.1)
        }

        pub fn size(&self) -> usize {
            self.range.0.len() + self.range.1.len() + std::mem::size_of::<usize>()
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
