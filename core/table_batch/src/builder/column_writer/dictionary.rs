use generated_types::influxdata::pbdata::v1::InternedStrings;
use hashbrown::{HashMap, hash_map::EntryRef};

use super::string::StringBuffer;

/// A dictionary encoded string buffer.
#[derive(Debug, Default)]
pub struct DictionaryBuffer {
    /// A dedupe map that holds all dictionary keys mapped to their dictionary
    /// ID / positional index into the [`StringBuffer`].
    id_map: HashMap<String, u32>,

    /// The encoded set of dictionary keys.
    keys: StringBuffer,

    /// The set of dictionary ID keys representing the column values.
    pub(crate) encoded: Vec<u32>,
}

impl DictionaryBuffer {
    pub(super) fn new(value: &str) -> Self {
        let mut s = Self::default();
        s.push_str(value);
        s
    }

    pub(crate) fn push_str(&mut self, value: &str) {
        let dict_count = self.id_map.len();

        // Lookup (or create) the dictionary ID for this value
        let id = match self.id_map.entry_ref(value) {
            EntryRef::Occupied(v) => {
                // This key already exists in the value buffer.
                *v.get()
            }
            EntryRef::Vacant(v) => {
                // Generate the next dictionary ID.
                let id = dict_count as u32;

                // retain the tag -> dictionary ID mapping.
                v.insert(id);

                // Add the new dictionary value to the string buffer.
                self.keys.push_str(value);

                id
            }
        };

        self.encoded.push(id);
    }

    pub(crate) fn len(&self) -> usize {
        self.encoded.len()
    }

    /// Remove the last wrote value.
    ///
    /// # Reference Leak
    ///
    /// This call removes the encoded value from the buffer.
    ///
    /// If this call removes the sole reference to a value in this buffer, the value will not appear
    /// in the decoded result, and is removed from the buffer containing all previously-pushed
    /// strings. This allows us to avoid storing and transmitting unnecessary data, at the cost of
    /// a slightly more expensive operation when we have to remove a value due to recovering from a
    /// partial write.
    ///
    /// # Panics
    ///
    /// Panics if no values remain in `self`.
    pub(crate) fn drop_last_value(&mut self) {
        let last_pushed = self.encoded.pop()
            .expect("If you call `drop_last_value`, the tag buffer must contain at least one value, but it contained zero");

        // And we should remove it from the packed strings as well if it failed, just to get more
        // predictable behavior and avoid sending some data if possible.
        if !self.encoded.contains(&last_pushed) {
            let removed = self.keys.pop()
                .expect("If we can remove a value from the interned strings, we must also be able to remove a value from the packed strings.");

            self.id_map.remove(&removed);
        }
    }

    pub(crate) fn finish(self) -> Option<InternedStrings> {
        let dictionary = self.keys.finish()?;

        // Invariant: the key -> ID map and the buffer containing the encoded
        // keys must always agree on the number of unique keys observed.
        debug_assert_eq!(
            self.id_map.len(),
            dictionary.offsets.len() - 1, // 0 starting offset
        );

        if self.encoded.is_empty() {
            None
        } else {
            Some(InternedStrings {
                dictionary: Some(dictionary),
                values: self.encoded,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{super::tests::*, *};
    use crate::{builder::Builder, values::InternedStr};
    use hashbrown::HashSet;
    use pretty_assertions::assert_eq;
    use proptest::proptest;

    #[test]
    fn test_default() {
        let b = DictionaryBuffer::default();

        // Unfinished repr.
        assert!(b.encoded.is_empty());
        assert!(b.id_map.is_empty());

        // Finished repr.
        assert_eq!(b.finish(), None);
    }

    /// This test demonstrates that there is no leak of dictionary values with no referents in the
    /// encoded data - see [`DictionaryBuffer::drop_last_value()`] docs.
    #[test]
    fn test_no_leak_unused_after_drop() {
        let mut b = DictionaryBuffer::default();

        b.push_str("bananas");
        b.push_str("platanos");
        b.drop_last_value(); // Drop "platanos"

        assert_eq!(b.encoded, vec![0]);

        let mut keys = b.id_map.into_iter().collect::<Vec<_>>();
        keys.sort();
        assert_eq!(keys, [("bananas".to_string(), 0)]);
    }

    proptest! {
        #[test]
        fn prop_buffer_ops(
            ops in proptest::collection::vec(arbitrary_op::<String>(), 1..20),
        ) {
            let mut dict = DictionaryBuffer::default();
            let mut control = Vec::default();

            for op in ops {
                match op {
                    Op::Push(v) => {
                        dict.push(InternedStr(&v));

                        let id = dict.id_map.get(&v).expect("must have key in dict");
                        assert_eq!(dict.encoded.last().expect("must encode value"), id);

                        control.push(v);
                    }
                    Op::Drop => {
                        // Overwriting the last value when there are no values is
                        // not a valid operation.
                        if control.is_empty() {
                            assert!(dict.encoded.is_empty());
                            continue;
                        }

                        dict.drop_last_value();
                        control.pop();
                    }
                }
            }

            let id_map = dict.id_map.clone();

            let wire = dict
                .finish()
                .unwrap_or_default();

            let control_ids = control
                .iter()
                .map(|v| *id_map.get(v).expect("dict must contain key"))
                .collect::<Vec<_>>();

            assert_eq!(wire.values, control_ids);

            // Assert that the ID set contains all the distinct values in the
            // control set.
            //
            // This cannot be an equality assertion, as the on-wire dictionary
            // encoding is "dirty", containing entries for keys later dropped,
            // unused.
            let control_set = control.iter().collect::<HashSet<_>>();
            let id_set = id_map.keys().collect::<HashSet<_>>();
            assert!(id_set.is_superset(&control_set));
        }
    }
}
