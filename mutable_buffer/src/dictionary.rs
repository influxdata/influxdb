//! Contains a structure to map from strings to u32 symbols based on
//! string interning.
use arrow::array::{ArrayDataBuilder, StringArray};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use hashbrown::HashMap;
use snafu::{OptionExt, Snafu};
use std::convert::TryInto;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Dictionary lookup error on id {}", id))]
    DictionaryIdLookupError { id: DID },

    #[snafu(display("Dictionary lookup error for value {}", value))]
    DictionaryValueLookupError { value: String },
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A "dictionary ID" (DID) is a compact numeric representation of an interned
/// string in the dictionary. The same string always maps the same DID.
///
/// DIDs can be compared, hashed and cheaply copied around, just like small integers.
///
/// An i32 is used to match the default for Arrow dictionaries
#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[allow(clippy::upper_case_acronyms)]
pub struct DID(i32);

impl DID {
    /// An invalid DID
    pub const fn invalid() -> Self {
        Self(-1)
    }

    pub fn as_i32(&self) -> i32 {
        self.0
    }
}

impl std::fmt::Display for DID {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug)]
struct Storage {
    /// The end of strings stored in storage
    ///
    /// An i32 is used to match the default arrow string arrays
    offsets: Vec<i32>,
    /// A contiguous array of string data
    storage: String,
}

impl Storage {
    fn new() -> Self {
        Self {
            offsets: vec![0],
            storage: String::new(),
        }
    }

    fn append(&mut self, data: &str) -> DID {
        let id = self.offsets.len() - 1;
        let id = id.try_into().expect("failed to fit id into i32");

        let offset = self.storage.len() + data.len();
        let offset = offset.try_into().expect("failed to fit offset into i32");

        self.offsets.push(offset);
        self.storage.push_str(data);

        DID(id)
    }

    fn get(&self, id: DID) -> Option<&str> {
        let id: usize = id.0.try_into().ok()?;

        let start_offset = *self.offsets.get(id)? as usize;
        let end_offset = *self.offsets.get(id + 1)? as usize;

        Some(&self.storage[start_offset..end_offset])
    }

    fn size(&self) -> usize {
        self.storage.len() + self.offsets.len() * std::mem::size_of::<i32>()
    }

    fn to_arrow(&self) -> StringArray {
        let len = self.offsets.len() - 1;
        let offsets = Buffer::from_slice_ref(&self.offsets);
        let values = Buffer::from(self.storage.as_bytes());

        let data = ArrayDataBuilder::new(DataType::Utf8)
            .len(len)
            .add_buffer(offsets)
            .add_buffer(values)
            .build();

        StringArray::from(data)
    }
}

/// An interning dictionary heavily inspired by the string-interner crate
#[derive(Debug)]
pub struct Dictionary {
    hash: ahash::RandomState,
    /// Used to provide a lookup from string value to DID
    ///
    /// Note: DID's hash implementation is not used, instead the raw entry
    /// API is used to store DID keys w.r.t the hash of the strings themselves
    ///
    dedup: HashMap<DID, (), ()>,
    /// Used to store strings
    storage: Storage,
}

impl Default for Dictionary {
    fn default() -> Self {
        Self::new()
    }
}

impl Dictionary {
    pub fn new() -> Self {
        Self {
            hash: ahash::RandomState::new(),
            dedup: Default::default(),
            storage: Storage::new(),
        }
    }

    /// Returns the id corresponding to value, adding an entry for the
    /// id if it is not yet present in the dictionary.
    pub fn lookup_value_or_insert(&mut self, value: &str) -> DID {
        use hashbrown::hash_map::RawEntryMut;

        let hasher = &self.hash;
        let storage = &mut self.storage;
        let hash = hash_str(hasher, value);

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |symbol| value == storage.get(*symbol).unwrap());

        match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let symbol = storage.append(value);
                *entry
                    .insert_with_hasher(hash, symbol, (), |symbol| {
                        let string = storage.get(*symbol).unwrap();
                        hash_str(hasher, string)
                    })
                    .0
            }
        }
    }

    /// Returns the ID in self.dictionary that corresponds to `value`,
    /// if any. No error is returned to avoid an allocation when no value is
    /// present
    pub fn id(&self, value: &str) -> Option<DID> {
        let hash = hash_str(&self.hash, value);
        self.dedup
            .raw_entry()
            .from_hash(hash, |symbol| value == self.storage.get(*symbol).unwrap())
            .map(|(&symbol, &())| symbol)
    }

    /// Returns the ID in self.dictionary that corresponds to `value`, if any.
    /// Returns an error if no such value is found. Does not add the value
    /// to the dictionary.
    pub fn lookup_value(&self, value: &str) -> Result<DID> {
        self.id(value).context(DictionaryValueLookupError { value })
    }

    /// Returns the str in self.dictionary that corresponds to `id`,
    /// if any. Returns an error if no such id is found
    pub fn lookup_id(&self, id: DID) -> Result<&str> {
        self.storage.get(id).context(DictionaryIdLookupError { id })
    }

    pub fn size(&self) -> usize {
        self.storage.size() + self.dedup.len() * std::mem::size_of::<DID>()
    }

    /// Returns the representation of this dictionaries values
    /// as an non-nullable, unsorted arrow StringArray
    #[allow(dead_code)] // Temporary
    pub fn to_arrow(&self) -> StringArray {
        self.storage.to_arrow()
    }
}

fn hash_str(hasher: &ahash::RandomState, value: &str) -> u64 {
    use std::hash::{BuildHasher, Hash, Hasher};
    let mut state = hasher.build_hasher();
    value.hash(&mut state);
    state.finish()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_storage() {
        let mut storage = Storage::new();

        let id1 = storage.append("hello");
        let id2 = storage.append("world");
        let id3 = storage.append("cupcake");

        assert!(storage.get(DID::invalid()).is_none());
        assert_eq!(storage.get(id1).unwrap(), "hello");
        assert_eq!(storage.get(id2).unwrap(), "world");
        assert_eq!(storage.get(id3).unwrap(), "cupcake");
    }

    #[test]
    fn test_dictionary() {
        let mut dictionary = Dictionary::new();

        let id1 = dictionary.lookup_value_or_insert("cupcake");
        let id2 = dictionary.lookup_value_or_insert("cupcake");
        let id3 = dictionary.lookup_value_or_insert("womble");

        let id4 = dictionary.lookup_value("cupcake").unwrap();
        let id5 = dictionary.lookup_value("womble").unwrap();

        let cupcake = dictionary.lookup_id(id4).unwrap();
        let womble = dictionary.lookup_id(id5).unwrap();

        let arrow_expected = StringArray::from(vec!["cupcake", "womble"]);
        let arrow_actual = dictionary.to_arrow();

        assert_eq!(id1, id2);
        assert_eq!(id1, id4);
        assert_ne!(id1, id3);
        assert_eq!(id3, id5);

        assert_eq!(cupcake, "cupcake");
        assert_eq!(womble, "womble");

        assert!(dictionary.id("foo").is_none());
        assert_eq!(arrow_expected, arrow_actual);
    }
}
