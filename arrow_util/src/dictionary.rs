//! Contains a structure to map from strings to integer symbols based on
//! string interning.
use std::convert::TryFrom;

use arrow::array::{Array, ArrayDataBuilder, DictionaryArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, Int32Type};
use hashbrown::HashMap;
use num_traits::{AsPrimitive, FromPrimitive, Zero};
use snafu::Snafu;

use crate::string::PackedStringArray;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("duplicate key found {}", key))]
    DuplicateKeyFound { key: String },
}

/// A String dictionary that builds on top of `PackedStringArray` adding O(1)
/// index lookups for a given string
///
/// Heavily inspired by the string-interner crate
#[derive(Debug, Clone)]
pub struct StringDictionary<K> {
    hash: ahash::RandomState,
    /// Used to provide a lookup from string value to key type
    ///
    /// Note: K's hash implementation is not used, instead the raw entry
    /// API is used to store keys w.r.t the hash of the strings themselves
    ///
    dedup: HashMap<K, (), ()>,
    /// Used to store strings
    storage: PackedStringArray<K>,
}

impl<K: AsPrimitive<usize> + FromPrimitive + Zero> Default for StringDictionary<K> {
    fn default() -> Self {
        Self {
            hash: ahash::RandomState::new(),
            dedup: Default::default(),
            storage: PackedStringArray::new(),
        }
    }
}

impl<K: AsPrimitive<usize> + FromPrimitive + Zero> StringDictionary<K> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn with_capacity(keys: usize, values: usize) -> StringDictionary<K> {
        Self {
            hash: Default::default(),
            dedup: HashMap::with_capacity_and_hasher(keys, ()),
            storage: PackedStringArray::with_capacity(keys, values),
        }
    }

    /// Returns the id corresponding to value, adding an entry for the
    /// id if it is not yet present in the dictionary.
    pub fn lookup_value_or_insert(&mut self, value: &str) -> K {
        use hashbrown::hash_map::RawEntryMut;

        let hasher = &self.hash;
        let storage = &mut self.storage;
        let hash = hash_str(hasher, value);

        let entry = self
            .dedup
            .raw_entry_mut()
            .from_hash(hash, |key| value == storage.get(key.as_()).unwrap());

        match entry {
            RawEntryMut::Occupied(entry) => *entry.into_key(),
            RawEntryMut::Vacant(entry) => {
                let index = storage.append(value);
                let key =
                    K::from_usize(index).expect("failed to fit string index into dictionary key");
                *entry
                    .insert_with_hasher(hash, key, (), |key| {
                        let string = storage.get(key.as_()).unwrap();
                        hash_str(hasher, string)
                    })
                    .0
            }
        }
    }

    /// Returns the ID in self.dictionary that corresponds to `value`, if any.
    pub fn lookup_value(&self, value: &str) -> Option<K> {
        let hash = hash_str(&self.hash, value);
        self.dedup
            .raw_entry()
            .from_hash(hash, |key| value == self.storage.get(key.as_()).unwrap())
            .map(|(&symbol, &())| symbol)
    }

    /// Returns the str in self.dictionary that corresponds to `id`
    pub fn lookup_id(&self, id: K) -> Option<&str> {
        self.storage.get(id.as_())
    }

    pub fn size(&self) -> usize {
        self.storage.size() + self.dedup.len() * std::mem::size_of::<K>()
    }

    pub fn values(&self) -> &PackedStringArray<K> {
        &self.storage
    }

    pub fn into_inner(self) -> PackedStringArray<K> {
        self.storage
    }

    /// Truncates this dictionary removing all keys larger than `id`
    pub fn truncate(&mut self, id: K) {
        let id = id.as_();
        self.dedup.retain(|k, _| k.as_() <= id);
        self.storage.truncate(id + 1)
    }

    /// Clears this dictionary removing all elements
    pub fn clear(&mut self) {
        self.storage.clear();
        self.dedup.clear()
    }
}

fn hash_str(hasher: &ahash::RandomState, value: &str) -> u64 {
    use std::hash::{BuildHasher, Hash, Hasher};
    let mut state = hasher.build_hasher();
    value.hash(&mut state);
    state.finish()
}

impl StringDictionary<i32> {
    /// Convert to an arrow representation with the provided set of
    /// keys and an optional null bitmask
    pub fn to_arrow<I>(&self, keys: I, nulls: Option<NullBuffer>) -> DictionaryArray<Int32Type>
    where
        I: IntoIterator<Item = i32>,
        I::IntoIter: ExactSizeIterator,
    {
        // the nulls are recorded in the keys array, the dictionary itself
        // is entirely non null
        let dictionary_nulls = None;
        let keys = keys.into_iter();

        let array_data = ArrayDataBuilder::new(DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        ))
        .len(keys.len())
        .add_buffer(keys.collect())
        .add_child_data(self.storage.to_arrow(dictionary_nulls).into_data())
        .nulls(nulls)
        // TODO consider skipping the validation checks by using
        // `build_unchecked()`
        .build()
        .expect("Valid array data");

        DictionaryArray::<Int32Type>::from(array_data)
    }
}

impl<K> TryFrom<PackedStringArray<K>> for StringDictionary<K>
where
    K: AsPrimitive<usize> + FromPrimitive + Zero,
{
    type Error = Error;

    fn try_from(storage: PackedStringArray<K>) -> Result<Self, Error> {
        use hashbrown::hash_map::RawEntryMut;

        let hasher = ahash::RandomState::new();
        let mut dedup: HashMap<K, (), ()> = HashMap::with_capacity_and_hasher(storage.len(), ());
        for (idx, value) in storage.iter().enumerate() {
            let hash = hash_str(&hasher, value);

            let entry = dedup
                .raw_entry_mut()
                .from_hash(hash, |key| value == storage.get(key.as_()).unwrap());

            match entry {
                RawEntryMut::Occupied(_) => {
                    return Err(Error::DuplicateKeyFound {
                        key: value.to_string(),
                    })
                }
                RawEntryMut::Vacant(entry) => {
                    let key =
                        K::from_usize(idx).expect("failed to fit string index into dictionary key");

                    entry.insert_with_hasher(hash, key, (), |key| {
                        let string = storage.get(key.as_()).unwrap();
                        hash_str(&hasher, string)
                    });
                }
            }
        }

        Ok(Self {
            hash: hasher,
            dedup,
            storage,
        })
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryInto;

    use super::*;

    #[test]
    fn test_dictionary() {
        let mut dictionary = StringDictionary::<i32>::new();

        let id1 = dictionary.lookup_value_or_insert("cupcake");
        let id2 = dictionary.lookup_value_or_insert("cupcake");
        let id3 = dictionary.lookup_value_or_insert("womble");

        let id4 = dictionary.lookup_value("cupcake").unwrap();
        let id5 = dictionary.lookup_value("womble").unwrap();

        let cupcake = dictionary.lookup_id(id4).unwrap();
        let womble = dictionary.lookup_id(id5).unwrap();

        let arrow_expected = arrow::array::StringArray::from(vec!["cupcake", "womble"]);
        let arrow_actual = dictionary.values().to_arrow(None);

        assert_eq!(id1, id2);
        assert_eq!(id1, id4);
        assert_ne!(id1, id3);
        assert_eq!(id3, id5);

        assert_eq!(cupcake, "cupcake");
        assert_eq!(womble, "womble");

        assert!(dictionary.lookup_value("foo").is_none());
        assert!(dictionary.lookup_id(-1).is_none());
        assert_eq!(arrow_expected, arrow_actual);
    }

    #[test]
    fn from_string_array() {
        let mut data = PackedStringArray::<u64>::new();
        data.append("cupcakes");
        data.append("foo");
        data.append("bingo");

        let dictionary: StringDictionary<_> = data.try_into().unwrap();

        assert_eq!(dictionary.lookup_value("cupcakes"), Some(0));
        assert_eq!(dictionary.lookup_value("foo"), Some(1));
        assert_eq!(dictionary.lookup_value("bingo"), Some(2));

        assert_eq!(dictionary.lookup_id(0), Some("cupcakes"));
        assert_eq!(dictionary.lookup_id(1), Some("foo"));
        assert_eq!(dictionary.lookup_id(2), Some("bingo"));
    }

    #[test]
    fn from_string_array_duplicates() {
        let mut data = PackedStringArray::<u64>::new();
        data.append("cupcakes");
        data.append("foo");
        data.append("bingo");
        data.append("cupcakes");

        let err = TryInto::<StringDictionary<_>>::try_into(data).expect_err("expected failure");
        assert!(matches!(err, Error::DuplicateKeyFound { key } if &key == "cupcakes"))
    }

    #[test]
    fn test_truncate() {
        let mut dictionary = StringDictionary::<i32>::new();
        dictionary.lookup_value_or_insert("cupcake");
        dictionary.lookup_value_or_insert("cupcake");
        dictionary.lookup_value_or_insert("bingo");
        let bingo = dictionary.lookup_value_or_insert("bingo");
        let bongo = dictionary.lookup_value_or_insert("bongo");
        dictionary.lookup_value_or_insert("bingo");
        dictionary.lookup_value_or_insert("cupcake");

        dictionary.truncate(bingo);

        assert_eq!(dictionary.values().len(), 2);
        assert_eq!(dictionary.dedup.len(), 2);

        assert_eq!(dictionary.lookup_value("cupcake"), Some(0));
        assert_eq!(dictionary.lookup_value("bingo"), Some(1));

        assert!(dictionary.lookup_value("bongo").is_none());
        assert!(dictionary.lookup_id(bongo).is_none());

        dictionary.lookup_value_or_insert("bongo");
        assert_eq!(dictionary.lookup_value("bongo"), Some(2));
    }
}
