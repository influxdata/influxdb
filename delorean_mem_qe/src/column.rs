use std::convert::From;

use super::encoding;

#[derive(Debug, PartialEq, PartialOrd)]
pub enum Scalar<'a> {
    String(&'a str),
    Float(f64),
    Integer(i64),
}

#[derive(Debug)]
pub enum Vector<'a> {
    String(Vec<&'a Option<std::string::String>>),
    Float(&'a [f64]),
    Integer(&'a [i64]),
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

    /// materialise all rows including and after row_id
    pub fn scan_from(&self, row_id: usize) -> Option<Vector> {
        if row_id >= self.num_rows() {
            println!(
                "asking for {:?} but only got {:?} rows",
                row_id,
                self.num_rows()
            );
            return None;
        }

        println!(
            "asking for {:?} with a column having {:?} rows",
            row_id,
            self.num_rows()
        );
        match self {
            Column::String(c) => Some(Vector::String(c.scan_from(row_id))),
            Column::Float(c) => Some(Vector::Float(c.scan_from(row_id))),
            Column::Integer(c) => Some(Vector::Integer(c.scan_from(row_id))),
        }
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

    // TODO(edd) shouldn't let roaring stuff leak out...
    fn row_ids(
        &self,
        value: Option<&Scalar>,
        order: std::cmp::Ordering,
    ) -> Option<croaring::Bitmap> {
        match self {
            Column::String(c) => match value {
                Some(scalar) => {
                    if let Scalar::String(v) = scalar {
                        Some(c.data.row_ids_roaring(Some(v.to_string())))
                    } else {
                        panic!("invalid value");
                    }
                }
                None => Some(c.data.row_ids_roaring(None)),
            },
            Column::Float(c) => {
                if let Some(Scalar::Float(v)) = value {
                    Some(c.data.row_ids_roaring(v, order))
                } else {
                    panic!("invalid value or unsupported null");
                }
            }
            Column::Integer(c) => {
                if let Some(Scalar::Integer(v)) = value {
                    Some(c.data.row_ids_roaring(v, order))
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

    pub fn scan_from(&self, row_id: usize) -> Vec<&Option<std::string::String>> {
        self.data.scan_from(row_id)
    }

    pub fn scan_from_until_some(&self, row_id: usize) -> Option<&std::string::String> {
        unreachable!("don't need this");
        // self.data.scan_from_until_some(row_id)
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

    pub fn scan_from(&self, row_id: usize) -> &[f64] {
        self.data.scan_from(row_id)
    }

    pub fn scan_from_until_some(&self, row_id: usize) -> Option<f64> {
        self.data.scan_from_until_some(row_id)
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
            println!(
                "column with ({:?}) maybe contain {:?} -- {:?}",
                self.range, v, res
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
