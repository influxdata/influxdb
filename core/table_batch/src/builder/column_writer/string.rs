use generated_types::influxdata::pbdata::v1::PackedStrings;
use influxdb_line_protocol::EscapedStr;

/// A [`StringBuffer`] packs string values into a single string buffer, with
/// each individual string tracked by an additional offset buffer.
#[derive(Debug)]
pub struct StringBuffer(pub(crate) PackedStrings);

impl Default for StringBuffer {
    fn default() -> Self {
        Self(PackedStrings {
            values: String::default(),
            offsets: vec![0],
        })
    }
}

impl StringBuffer {
    pub(super) fn new(value: &EscapedStr<'_>) -> Self {
        Self(PackedStrings {
            values: String::from(value.as_str()),
            offsets: vec![0, value.len() as u32],
        })
    }

    pub(crate) fn push_str(&mut self, s: &str) {
        self.0.values.push_str(s);
        self.0.offsets.push(self.0.values.len() as u32);
    }

    /// Remove the last wrote value and return it.
    ///
    /// This call does not panic when empty.
    pub(crate) fn pop(&mut self) -> Option<String> {
        if self.0.offsets.len() < 2 {
            return None;
        }

        self.0.offsets.pop();
        let new_len = *self.0.offsets.last().expect("must have offset") as usize;

        let last_val = self.0.values.split_at(new_len).1.to_string();
        self.0.values.truncate(new_len);

        Some(last_val)
    }

    pub(crate) fn len(&self) -> usize {
        self.0.offsets.len() - 1
    }

    pub(crate) fn finish(self) -> Option<PackedStrings> {
        if self.0.offsets.len() < 2 {
            None
        } else {
            Some(self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{builder::Builder, values::PackedStr};
    use proptest::proptest;

    use super::{super::tests::*, *};

    #[test]
    fn test_default() {
        let b = StringBuffer::default();

        // Unfinished repr.
        assert_eq!(b.0.values, "");
        assert_eq!(b.0.offsets, vec![0]);

        // Finished repr
        assert_eq!(StringBuffer::default().finish(), None);
    }

    #[test]
    fn test_packed_encoding() {
        let mut b = StringBuffer::default();

        b.push(PackedStr(""));
        b.push(PackedStr("hello"));
        b.push(PackedStr(" "));
        b.push(PackedStr("bananas"));
        b.push(PackedStr(""));
        b.push(PackedStr("platanos"));

        let got = b.finish().expect("non-empty column");

        assert_eq!(got.values, "hello bananasplatanos");
        assert_eq!(got.offsets.as_slice(), [0, 0, 5, 6, 13, 13, 21]);
    }

    /// Assert the behaviour of the [`StringBuffer`] with and empty strings
    /// matches that of the `MutableBatch`.
    #[test]
    fn test_nulls() {
        let mut b = StringBuffer::default();

        b.push_str("");
        b.push(PackedStr(""));
        b.push(PackedStr(""));
        b.push_str("");

        let got = b.finish().expect("non-empty column");

        assert_eq!(got.values, "");
        assert_eq!(got.offsets.as_slice(), [0, 0, 0, 0, 0]);
    }

    proptest! {
        #[test]
        fn prop_buffer_ops(
            ops in proptest::collection::vec(arbitrary_op::<String>(), 1..20),
        ) {
            let mut b = StringBuffer::default();
            let mut control = Vec::default();

            for op in ops {
                match op {
                    Op::Push(v) => {
                        b.push(PackedStr(&v));
                        control.push(v);
                    }
                    Op::Drop => {
                        // Overwriting the last value when there are no values is
                        // not a valid operation.
                        if control.is_empty() {
                            assert!(b.0.values.is_empty());
                            assert_eq!(b.0.offsets.len(), 1);
                            continue;
                        }

                        assert_eq!(b.pop(), control.pop());
                    }
                }

                assert_eq!(b.0.offsets.len() - 1, control.len());
            }

            let wire = b
                .finish()
                .unwrap_or_default();

            // Concatenate all the string values and assert the packed strings
            // match.
            let want_values = control.iter().fold(String::new(), |mut acc, v| {
                acc.push_str(v);
                acc
            });
            assert_eq!(wire.values, want_values);

            // Validate the offsets slice the concatenated string into the input
            // strings.
            for (idx, input) in control.iter().enumerate() {
                let start_idx = wire.offsets[idx] as usize;
                let end_idx = wire.offsets[idx + 1] as usize;

                let sliced = &wire.values[start_idx..end_idx];
                assert_eq!(sliced, input);
            }
        }
    }
}
