//! This module contains code to pack values into a format suitable
//! for feeding to the parquet writer. It is destined for replacement
//! by an Apache Arrow based implementation

// Note the maintainability of this code is not likely high (it came
// from the copy pasta factory) but the plan is to replace it
// soon... We'll see how long that actually takes...

use parquet::data_type::ByteArray;

// NOTE: See https://blog.twitter.com/engineering/en_us/a/2013/dremel-made-simple-with-parquet.html
// for an explination of nesting levels

/// Packs data for a column of strings
#[derive(Debug)]
pub struct StringPacker {
    pub values: Vec<ByteArray>,
    pub def_levels: Vec<i16>,
    pub rep_levels: Vec<i16>,
}
impl StringPacker {
    fn new() -> StringPacker {
        StringPacker {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> StringPacker {
        StringPacker {
            values: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // Adds (copies) the data to be encoded
    fn pack(&mut self, s: Option<&str>) {
        match s {
            Some(s) => {
                self.values.push(ByteArray::from(s));
                self.def_levels.push(1);
                self.rep_levels.push(1);
            }
            None => {
                self.values.push(ByteArray::from(""));
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }
}

// Packs data for a column of floats
#[derive(Debug)]
pub struct FloatPacker {
    pub values: Vec<f64>,
    pub def_levels: Vec<i16>,
    pub rep_levels: Vec<i16>,
}
impl FloatPacker {
    fn new() -> FloatPacker {
        FloatPacker {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> FloatPacker {
        FloatPacker {
            values: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // Adds (copies) the data to be encoded
    fn pack(&mut self, f: Option<f64>) {
        match f {
            Some(f) => {
                self.values.push(f);
                self.def_levels.push(1);
                self.rep_levels.push(1);
            }
            None => {
                self.values.push(std::f64::NAN); // doesn't matter as def level == 0
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }
}

// Packs data for a column of ints
#[derive(Debug)]
pub struct IntPacker {
    pub values: Vec<i64>,
    pub def_levels: Vec<i16>,
    pub rep_levels: Vec<i16>,
}
impl IntPacker {
    fn new() -> IntPacker {
        IntPacker {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> IntPacker {
        IntPacker {
            values: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // Adds (copies) the data to be encoded
    fn pack(&mut self, i: Option<i64>) {
        match i {
            Some(i) => {
                self.values.push(i);
                self.def_levels.push(1);
                self.rep_levels.push(1);
            }
            None => {
                self.values.push(0); // doesn't matter as def level == 0
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }
}

// Packs data for a column of bool
#[derive(Debug)]
pub struct BoolPacker {
    pub values: Vec<bool>,
    pub def_levels: Vec<i16>,
    pub rep_levels: Vec<i16>,
}
impl BoolPacker {
    fn new() -> BoolPacker {
        BoolPacker {
            values: Vec::new(),
            def_levels: Vec::new(),
            rep_levels: Vec::new(),
        }
    }

    fn with_capacity(capacity: usize) -> BoolPacker {
        BoolPacker {
            values: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
            rep_levels: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    // Adds (copies) the data to be encoded
    fn pack(&mut self, b: Option<bool>) {
        match b {
            Some(b) => {
                self.values.push(b);
                self.def_levels.push(1);
                self.rep_levels.push(1);
            }
            None => {
                self.values.push(false); // doesn't matter as def level == 0
                self.def_levels.push(0);
                self.rep_levels.push(1);
            }
        }
    }
}

#[derive(Debug)]
pub enum Packer {
    StringPackerType(StringPacker),
    FloatPackerType(FloatPacker),
    IntPackerType(IntPacker),
    BoolPackerType(BoolPacker),
}

impl Packer {
    /// Create a new packer that can pack values of the specified protocol type
    pub fn new(t: delorean_table_schema::DataType) -> Packer {
        match t {
            delorean_table_schema::DataType::String => {
                Packer::StringPackerType(StringPacker::new())
            }
            delorean_table_schema::DataType::Float => Packer::FloatPackerType(FloatPacker::new()),
            delorean_table_schema::DataType::Integer => Packer::IntPackerType(IntPacker::new()),
            delorean_table_schema::DataType::Boolean => Packer::BoolPackerType(BoolPacker::new()),
            delorean_table_schema::DataType::Timestamp => Packer::IntPackerType(IntPacker::new()),
        }
    }

    /// Create a new packer that can pack values of the specified type, with the specified capacity
    pub fn with_capacity(t: delorean_table_schema::DataType, capacity: usize) -> Packer {
        match t {
            delorean_table_schema::DataType::String => {
                Packer::StringPackerType(StringPacker::with_capacity(capacity))
            }
            delorean_table_schema::DataType::Float => {
                Packer::FloatPackerType(FloatPacker::with_capacity(capacity))
            }
            delorean_table_schema::DataType::Integer => {
                Packer::IntPackerType(IntPacker::with_capacity(capacity))
            }
            delorean_table_schema::DataType::Boolean => {
                Packer::BoolPackerType(BoolPacker::with_capacity(capacity))
            }
            delorean_table_schema::DataType::Timestamp => {
                Packer::IntPackerType(IntPacker::with_capacity(capacity))
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            Packer::StringPackerType(string_packer) => string_packer.len(),
            Packer::FloatPackerType(float_packer) => float_packer.len(),
            Packer::IntPackerType(int_packer) => int_packer.len(),
            Packer::BoolPackerType(bool_packer) => bool_packer.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        // Clippy made me put this
        self.len() == 0
    }

    pub fn pack_str(&mut self, s: Option<&str>) {
        if let Packer::StringPackerType(string_packer) = self {
            string_packer.pack(s)
        } else {
            panic!("Packer {:?} does not know how to pack strings", self);
        }
    }

    pub fn pack_f64(&mut self, f: Option<f64>) {
        if let Packer::FloatPackerType(float_packer) = self {
            float_packer.pack(f)
        } else {
            panic!("Packer {:?} does not know how to pack floats", self);
        }
    }

    pub fn pack_i64(&mut self, i: Option<i64>) {
        if let Packer::IntPackerType(int_packer) = self {
            int_packer.pack(i)
        } else {
            panic!("Packer {:?} does not know how to pack ints", self);
        }
    }

    pub fn pack_bool(&mut self, b: Option<bool>) {
        if let Packer::BoolPackerType(bool_packer) = self {
            bool_packer.pack(b)
        } else {
            panic!("Packer {:?} does not know how to pack bools", self);
        }
    }

    pub fn as_string_packer(&self) -> &StringPacker {
        if let Packer::StringPackerType(string_packer) = self {
            string_packer
        } else {
            panic!("Packer {:?} is not a string packer", self);
        }
    }

    pub fn as_float_packer(&self) -> &FloatPacker {
        if let Packer::FloatPackerType(float_packer) = self {
            float_packer
        } else {
            panic!("Packer {:?} is not a float packer", self);
        }
    }

    pub fn as_int_packer(&self) -> &IntPacker {
        if let Packer::IntPackerType(int_packer) = self {
            int_packer
        } else {
            panic!("Packer {:?} is not an int packer", self);
        }
    }

    pub fn as_bool_packer(&self) -> &BoolPacker {
        if let Packer::BoolPackerType(bool_packer) = self {
            bool_packer
        } else {
            panic!("Packer {:?} is not an bool packer", self);
        }
    }

    pub fn pack_none(&mut self) {
        match self {
            Packer::StringPackerType(string_packer) => string_packer.pack(None),
            Packer::FloatPackerType(float_packer) => float_packer.pack(None),
            Packer::IntPackerType(int_packer) => int_packer.pack(None),
            Packer::BoolPackerType(bool_packer) => bool_packer.pack(None),
        }
    }

    /// Return true if the idx'th row is null
    pub fn is_null(&self, idx: usize) -> bool {
        match self {
            Packer::StringPackerType(string_packer) => string_packer.def_levels[idx] == 0,
            Packer::FloatPackerType(float_packer) => float_packer.def_levels[idx] == 0,
            Packer::IntPackerType(int_packer) => int_packer.def_levels[idx] == 0,
            Packer::BoolPackerType(bool_packer) => bool_packer.def_levels[idx] == 0,
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use delorean_table_schema::DataType;
    use delorean_test_helpers::approximately_equal;

    #[test]
    fn with_capacity() {
        let string_packer = StringPacker::with_capacity(42);
        assert_eq!(string_packer.values.capacity(), 42);
        assert_eq!(string_packer.def_levels.capacity(), 42);
        assert_eq!(string_packer.rep_levels.capacity(), 42);

        let float_packer = FloatPacker::with_capacity(43);
        assert_eq!(float_packer.values.capacity(), 43);
        assert_eq!(float_packer.def_levels.capacity(), 43);
        assert_eq!(float_packer.rep_levels.capacity(), 43);

        let int_packer = IntPacker::with_capacity(44);
        assert_eq!(int_packer.values.capacity(), 44);
        assert_eq!(int_packer.def_levels.capacity(), 44);
        assert_eq!(int_packer.rep_levels.capacity(), 44);

        let bool_packer = BoolPacker::with_capacity(45);
        assert_eq!(bool_packer.values.capacity(), 45);
        assert_eq!(bool_packer.def_levels.capacity(), 45);
        assert_eq!(bool_packer.rep_levels.capacity(), 45);
    }

    #[test]
    fn string_packer() {
        let mut packer = Packer::new(DataType::String);
        assert_eq!(packer.len(), 0);
        packer.pack_str(Some("foo"));
        packer.pack_str(Some(""));
        packer.pack_str(None);
        packer.pack_none();
        packer.pack_str(Some("bar"));
        assert_eq!(packer.len(), 5);
        assert_eq!(
            packer.as_string_packer().values[0].as_utf8().unwrap(),
            "foo"
        );
        assert_eq!(packer.as_string_packer().values[1].as_utf8().unwrap(), "");
        assert_eq!(
            packer.as_string_packer().values[4].as_utf8().unwrap(),
            "bar"
        );
        assert_eq!(packer.as_string_packer().def_levels, vec![1, 1, 0, 0, 1]);
        assert_eq!(packer.as_string_packer().rep_levels, vec![1, 1, 1, 1, 1]);

        assert!(!packer.is_null(1));
        assert!(packer.is_null(2));
    }

    #[test]
    #[should_panic]
    fn string_packer_pack_float() {
        let mut packer = Packer::new(DataType::String);
        packer.pack_f64(Some(5.3));
    }

    #[test]
    fn float_packer() {
        let mut packer = Packer::new(DataType::Float);
        assert_eq!(packer.len(), 0);
        packer.pack_f64(Some(1.23));
        packer.pack_f64(None);
        packer.pack_none();
        packer.pack_f64(Some(4.56));
        assert_eq!(packer.len(), 4);
        assert!(approximately_equal(
            packer.as_float_packer().values[0],
            1.23
        ));
        assert!(approximately_equal(
            packer.as_float_packer().values[3],
            4.56
        ));
        assert_eq!(packer.as_float_packer().def_levels, vec![1, 0, 0, 1]);
        assert_eq!(packer.as_float_packer().rep_levels, vec![1, 1, 1, 1]);

        assert!(!packer.is_null(0));
        assert!(packer.is_null(1));
    }

    #[test]
    #[should_panic]
    fn float_packer_pack_string() {
        let mut packer = Packer::new(DataType::Float);
        packer.pack_str(Some("foo"));
    }

    fn test_int_packer(mut packer: Packer) {
        assert_eq!(packer.len(), 0);
        packer.pack_i64(Some(1));
        packer.pack_i64(None);
        packer.pack_none();
        packer.pack_i64(Some(-1));
        assert_eq!(packer.len(), 4);
        assert_eq!(packer.as_int_packer().values[0], 1);
        assert_eq!(packer.as_int_packer().values[3], -1);
        assert_eq!(packer.as_int_packer().def_levels, vec![1, 0, 0, 1]);
        assert_eq!(packer.as_int_packer().rep_levels, vec![1, 1, 1, 1]);

        assert!(!packer.is_null(0));
        assert!(packer.is_null(1));
    }

    #[test]
    fn int_packer() {
        let packer = Packer::new(DataType::Integer);
        test_int_packer(packer);
    }

    #[test]
    #[should_panic]
    fn int_packer_pack_string() {
        let mut packer = Packer::new(DataType::Integer);
        packer.pack_str(Some("foo"));
    }

    #[test]
    fn bool_packer() {
        let mut packer = Packer::new(DataType::Boolean);
        assert_eq!(packer.len(), 0);
        packer.pack_bool(Some(true));
        packer.pack_bool(Some(false));
        packer.pack_bool(None);
        packer.pack_none();
        packer.pack_bool(Some(true));
        assert_eq!(packer.len(), 5);
        assert_eq!(packer.as_bool_packer().values[0], true);
        assert_eq!(packer.as_bool_packer().values[1], false);
        assert_eq!(packer.as_bool_packer().values[4], true);
        assert_eq!(packer.as_bool_packer().def_levels, vec![1, 1, 0, 0, 1]);
        assert_eq!(packer.as_bool_packer().rep_levels, vec![1, 1, 1, 1, 1]);

        assert!(!packer.is_null(1));
        assert!(packer.is_null(2));
    }

    #[test]
    #[should_panic]
    fn bool_packer_pack_string() {
        let mut packer = Packer::new(DataType::Boolean);
        packer.pack_str(Some("foo"));
    }

    #[test]
    fn timstamp_packer() {
        let packer = Packer::new(DataType::Timestamp);
        test_int_packer(packer);
    }

    #[test]
    #[should_panic]
    fn timstamp_packer_pack_string() {
        let mut packer = Packer::new(DataType::Timestamp);
        packer.pack_str(Some("foo"));
    }
}
