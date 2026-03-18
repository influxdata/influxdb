/// A descriptive specification of "null" and "non-null" values.
#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum Value {
    Null,
    NonNull,
}

/// A NULL-mask builder that provides linear, lazy construction of a `Vec<u8>`
/// bitmap where all [`Value::Null`] values are stored as 1 bits, and
/// [`Value::NonNull`] values are stored as 0 bits.
///
/// The LSB of a u8 block is the 0 index, and the MSB is the 7 index:
///
/// ```text
///                            7      0  15     8
///                 null_mask: 00100000  00000001
/// ```
///
/// Where the above encodes the indexes 5 and 8.
///
/// Trailing bits beyond [`NullMaskBuilder::len`] are always 0, meaning the
/// number of 1 bits always equals the count of NULL values, but the number of 0
/// bits does not always equal the number of non-NULL values due to this 0
/// padding.
///
/// This [`NullMaskBuilder`] is optimised to encode runs of [`Value::NonNull`]
/// values; non-NULL values are lazily materialised iff a NULL bit is added. A
/// completely empty bitmap produced by this type (of length 0) is equal to "all
/// values are non-null" for any number of values stored.
///
/// This type is designed to efficiently construct NULL masks compatible with
/// the [`Column::null_mask`] RPC field without additional processing.
///
/// [`Column::null_mask`]:
///     generated_types::influxdata::pbdata::v1::Column::null_mask
#[derive(Debug, Default, PartialEq)]
pub(crate) struct NullMaskBuilder {
    /// 8-bit bitmap blocks that are lazily materialised when a 1-bit needs
    /// storing.
    ///
    /// If no NULLs / 1 bits are added, this vector is empty irrespective of the
    /// number of non-NULL values added to the bitmap.
    bitmap: Vec<u8>,

    /// The 1-based logical length of the bitmap.
    ///
    /// This may differ from the physical number of bits stored in `bitmap` due
    /// to the lazy growth of the bitmap.
    ///
    /// The next value to be wrote to is always 0-indexed by `len`.
    len: usize,
}

impl NullMaskBuilder {
    pub(crate) fn with_nulls_before_single_valid(preceding_nulls: usize) -> Self {
        let vec_size = preceding_nulls.div_ceil(8);
        // We just fill it with u8::MAX and then change the last one
        let mut bitmap = vec![u8::MAX; vec_size];

        // If it's a multiple of 8, we want multiple u8::MAX items, and then nothing after that.
        if !preceding_nulls.is_multiple_of(8) {
            // E.g. if 11 values are nulls, then this will be:
            // (1 << (11 % 8)) - 1
            // (1 << 3) - 1
            // 0b1000 - 1
            // 0b0111
            //
            // And then we get 3 1s! perfect!
            let final_ones = (1 << (preceding_nulls % 8)) - 1;
            bitmap[vec_size - 1] = final_ones;
        }

        Self {
            bitmap,
            len: preceding_nulls + 1,
        }
    }

    /// Append this [`Value`] to the null mask.
    ///
    /// This call is amortised `O(1)` for [`Value::Null`] and always `O(1)` and
    /// allocation free for [`Value::NonNull`]
    pub(crate) fn push(&mut self, bit: Value) {
        // Only NULL values require modifying the bitmap.
        if bit == Value::Null {
            self.append_null();
        }

        // Advance the bitmap length.
        self.len += 1;
    }

    #[cold]
    fn append_null(&mut self) {
        // Compute the 0-based index of the u8 block that holds this bit.
        let block_idx = self.len / 8;

        // Lazily grow the bitmap to contain this many blocks / bits.
        //
        // This enables lazy materialisation by appending any not-yet-wrote-to
        // bits as "not null" up until `self.len`.
        self.bitmap.resize(block_idx + 1, 0);

        // A 1 bit has to be added to the NULL mask to indicate a NULL value.
        self.bitmap[block_idx] |= 1 << (self.len % 8);
    }

    /// Remove the last [`Value`] from this bitmap.
    ///
    /// This operation is `O(1)` unless the resulting bitmap contains trailing 0
    /// bytes, where it is `O(bytes)`.
    ///
    /// # Panics
    ///
    /// Panics if this bitmap is of length 0.
    pub(super) fn pop(&mut self) -> Value {
        assert_ne!(self.len, 0);

        // Decrement the length by 1 first, so that `self.len` indexes to the
        // last wrote bit.
        self.len -= 1;

        // Compute the 0-based index of the u8 block that holds this bit.
        let block_idx = self.len / 8;

        // Read and clear the bit, if it exists.
        let mut bit_set = false;
        if let Some(block) = self.bitmap.get_mut(block_idx) {
            let mask = 1 << (self.len % 8);
            // Read the bit
            bit_set = *block & mask > 0;
            // Clear the bit.
            *block &= !mask;
        }

        // Optimisation: trim all empty blocks from the end of the bitmap to
        // avoid them being sent over the wire if no further NULLs are wrote.
        while self.bitmap.last().map(|&v| v == 0).unwrap_or_default() {
            self.bitmap.pop();
        }

        // Yield the stored Value that was just removed.
        match bit_set {
            true => Value::Null,
            false => Value::NonNull,
        }
    }

    /// Return the logical length of this [`NullMaskBuilder`] (total number of
    /// null and non-null [`Value`] encoded).
    ///
    /// This call is always `O(1)`.
    ///
    /// Note: the physical length may differ due to lazily materialisation.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    /// Return `true` if no [`Value`] have been encoded into this
    /// [`NullMaskBuilder`].
    ///
    /// This call is always `O(1)`.
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Consume this [`NullMaskBuilder`] and return the constructed null mask
    /// bitmap.
    ///
    /// This call is always `O(1)`.
    ///
    /// NOTE: this may have length of 0 if no NULL values were pushed.
    pub(super) fn finish(self) -> Vec<u8> {
        self.bitmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::bitset::BitSet;
    use proptest::prelude::*;

    #[test]
    fn test_single_block_construction() {
        let mut b = NullMaskBuilder::default();
        assert!(b.is_empty());

        b.push(Value::NonNull); // bit 1
        b.push(Value::NonNull); // bit 2
        b.push(Value::Null); // bit 3
        b.push(Value::NonNull); // bit 4

        assert_eq!(b.len(), 4);
        assert!(!b.is_empty());

        match *b.finish() {
            [0b00000100] => {}
            [_, ..] => panic!("too many blocks"),
            _ => panic!("wrong bitmap bits"),
        }
    }

    #[test]
    fn test_multi_block_construction() {
        let mut b = NullMaskBuilder::default();
        assert!(b.is_empty());

        b.push(Value::NonNull); // bit 1
        b.push(Value::NonNull); // bit 2
        b.push(Value::NonNull); // bit 3
        b.push(Value::NonNull); // bit 4
        b.push(Value::Null); // bit 5
        b.push(Value::NonNull); // bit 6
        b.push(Value::NonNull); // bit 7
        b.push(Value::NonNull); // bit 8
        b.push(Value::NonNull); // bit 9
        b.push(Value::NonNull); // bit 10
        b.push(Value::NonNull); // bit 11
        b.push(Value::NonNull); // bit 12
        b.push(Value::Null); // bit 13

        assert_eq!(b.len(), 13);
        assert!(!b.is_empty());

        match *b.finish() {
            [0b00010000, 0b00010000] => {}
            [_, _, ..] => panic!("too many blocks"),
            _ => panic!("wrong bitmap block"),
        }
    }

    #[test]
    fn test_lazy_block_construction() {
        let mut b = NullMaskBuilder::default();
        assert!(b.is_empty());

        b.push(Value::NonNull); // bit 1
        b.push(Value::NonNull); // bit 2
        b.push(Value::NonNull); // bit 3
        b.push(Value::NonNull); // bit 4
        b.push(Value::NonNull); // bit 5
        b.push(Value::NonNull); // bit 6
        b.push(Value::NonNull); // bit 7
        b.push(Value::NonNull); // bit 8
        b.push(Value::NonNull); // bit 9
        b.push(Value::NonNull); // bit 10
        b.push(Value::NonNull); // bit 11
        b.push(Value::NonNull); // bit 12
        b.push(Value::NonNull); // bit 13

        assert_eq!(b.len(), 13);
        assert!(!b.is_empty());

        // As an optimisation, a null mask containing encoding non-null values
        // can be represented as empty bitmap.
        assert!(b.bitmap.is_empty());
        assert!(b.finish().is_empty());
    }

    #[test]
    fn test_lazy_block_materialisation() {
        let mut b = NullMaskBuilder::default();
        assert!(b.is_empty());

        b.push(Value::NonNull); // bit 1
        b.push(Value::NonNull); // bit 2
        b.push(Value::NonNull); // bit 3
        b.push(Value::NonNull); // bit 4
        b.push(Value::NonNull); // bit 5
        b.push(Value::NonNull); // bit 6
        b.push(Value::NonNull); // bit 7
        b.push(Value::NonNull); // bit 8
        b.push(Value::NonNull); // bit 9
        b.push(Value::NonNull); // bit 10
        b.push(Value::NonNull); // bit 11
        b.push(Value::NonNull); // bit 12

        // As an optimisation, a null mask containing encoding non-null values
        // can be represented as empty bitmap up until the first NULL value is
        // encoded.
        assert!(b.bitmap.is_empty());
        assert_eq!(b.len(), 12);
        assert!(!b.is_empty());

        // Append a NULL
        b.push(Value::Null); // bit 13
        assert_eq!(b.len(), 13);
        assert!(!b.is_empty());

        // At which point the non-null values prior to it are immediately
        // materialised as 0-bits.
        match *b.finish() {
            [0b00000000, 0b00010000] => {}
            [_, _, ..] => panic!("too many blocks"),
            _ => panic!("wrong bitmap block"),
        }
    }

    #[test]
    fn initial_construction_is_correct() {
        let orig = NullMaskBuilder::with_nulls_before_single_valid(12);
        assert_eq!(
            orig,
            NullMaskBuilder {
                bitmap: vec![0b11111111, 0b00001111],
                len: 13
            }
        );

        let orig = NullMaskBuilder::with_nulls_before_single_valid(7);
        assert_eq!(
            orig,
            NullMaskBuilder {
                bitmap: vec![0b01111111],
                len: 8
            }
        );

        let mut orig = NullMaskBuilder::with_nulls_before_single_valid(0);
        assert_eq!(
            orig,
            NullMaskBuilder {
                bitmap: vec![],
                len: 1
            }
        );

        orig.push(Value::NonNull);
        orig.push(Value::Null);

        assert_eq!(
            orig,
            NullMaskBuilder {
                bitmap: vec![0b00000100],
                len: 3
            }
        );

        let orig = NullMaskBuilder::with_nulls_before_single_valid(8);
        assert_eq!(
            orig,
            NullMaskBuilder {
                bitmap: vec![0b11111111],
                len: 9
            }
        );
    }

    fn arbitrary_value_mask() -> impl Strategy<Value = Value> {
        any::<bool>().prop_map(|v| match v {
            true => Value::Null,
            false => Value::NonNull,
        })
    }

    #[derive(Debug, Clone)]
    enum Op {
        Push(Value),
        Pop,
    }

    fn arbitrary_bitset_op() -> impl Strategy<Value = Op> {
        prop_oneof![arbitrary_value_mask().prop_map(Op::Push), Just(Op::Pop)]
    }

    proptest! {
        #[test]
        fn prop_bit_count(
            values in prop::collection::vec(arbitrary_value_mask(), 0..20),
        ) {
            let mut b = NullMaskBuilder::default();
            assert!(b.is_empty());

            for &v in &values {
                b.push(v);
                assert!(!b.is_empty());
            }

            // Invariant: the number of values pushed must match the logical
            // bitmap length.
            assert_eq!(b.len(), values.len());

            let bitmap = b.finish();

            // Invariant: the number of set bits equals the number of NULL
            // values.
            let null_count = values.iter().filter(|v| matches!(v, Value::Null)).count();
            let set_bits = bitmap.iter().map(|v| v.count_ones()).sum::<u32>();
            assert_eq!(null_count, set_bits as usize);

            // Optimisation: if the number of nulls is 0, the length of the
            // bitmap produced is 0 (lazy construction).
            assert_eq!(null_count == 0, bitmap.is_empty())
        }

        /// Assert byte equivalence between the bitmap constructed using the
        /// [`NullMaskBuilder`] and the bespoke influx [`BitSet`].
        #[test]
        fn prop_influx_bitset_equivalence(
            values in prop::collection::vec(arbitrary_value_mask(), 0..20),
        ) {
            let mut b = NullMaskBuilder::default();
            let mut control = BitSet::default();

            for &v in &values {
                b.push(v);

                match v {
                    Value::Null => control.append_set(1),
                    Value::NonNull => control.append_unset(1),
                }
            }

            // Append a NULL to both bitmaps to force initialisation of the
            // lazily built null mask.
            b.push(Value::Null);
            control.append_set(1);

            assert_eq!(b.len(), control.len());

            let bitmap = b.finish();

            // Invariant: this optimised constructor and the bitset must produce
            // identical bytes.
            assert_eq!(bitmap, control.bytes());
        }

        /// Drive random push & pop operations and compare the result to a
        /// known-good implementation.
        #[test]
        fn prop_push_pop(
            ops in prop::collection::vec(arbitrary_bitset_op(), 1..20),
        ) {
            let mut b = NullMaskBuilder::default();
            let mut control = BitSet::default();

            for op in ops {
                assert_eq!(b.len(), control.len());

                match op {
                    Op::Push(value) => {
                        b.push(value);

                        // Map the value to a bit for the control bitmap.
                        match value {
                            Value::Null => control.append_set(1),
                            Value::NonNull => control.append_unset(1),
                        }
                    },
                    Op::Pop if !b.is_empty() => {
                        let control_got = control.get(control.len().saturating_sub(1));
                        let got = b.pop();
                        control.truncate(control.len().saturating_sub(1));

                        // Map the value to a bit used by the control bitmap.
                        let got = match got {
                            Value::Null => true,
                            Value::NonNull => false,
                        };

                        // Assert the pop yielded the correct value.
                        assert_eq!(got, control_got);
                    }
                    Op::Pop => {
                        // An empty bitmap can't be popped.
                    }
                }
            }

            // Append a NULL to both bitmaps to force initialisation of the
            // lazily built null mask.
            b.push(Value::Null);
            control.append_set(1);

            assert_eq!(b.len(), control.len());

            let bitmap = b.finish();

            // Invariant: this optimised constructor and the bitset must produce
            // identical bytes.
            assert_eq!(bitmap, control.bytes());
        }
    }
}
