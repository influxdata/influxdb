/// A module containing the builder for columns specifically
pub mod column_writer;
pub(super) mod null_mask;

use core::{
    fmt::Debug,
    ops::{Not, Range},
};

use bitvec::{prelude::Lsb0, view::BitView};
pub use column_writer::{dictionary::DictionaryBuffer, string::StringBuffer};
use generated_types::influxdata::pbdata::v1::{InternedStrings, PackedStrings};

use crate::{
    ValueCollection,
    builder::null_mask::{NullMaskBuilder, Value},
    values::{InternedStr, PackedStr},
};

/// A trait to represent a value which is being used to produce another value, specifically for use
/// within partitioning and creating table batches.
pub trait Builder<Item>: Default + std::fmt::Debug {
    /// The final type that this builder produces, generally a vec-like type.
    type Output;
    /// The type of values which ranges of `Item` can be pulled from to further build our
    /// [`Self::Output`]
    type ExtendableFrom: ?Sized + std::fmt::Debug;

    /// Push a single item to this buffer
    fn push(&mut self, item: Item);
    /// Pull the specified range from `from` and clone/copy it into self.
    fn extend_from_range(&mut self, range: Range<usize>, from: &Self::ExtendableFrom);
    /// Produce the final built value. If no values were written to this builder, this must return
    /// None.
    fn finish(self) -> Option<Self::Output>;
}

impl<T: Clone + Debug> Builder<T> for Vec<T> {
    type Output = Self;
    type ExtendableFrom = [T];
    fn push(&mut self, item: T) {
        Self::push(self, item)
    }

    fn extend_from_range(&mut self, range: Range<usize>, from: &Self::ExtendableFrom) {
        let Some(items) = from.get(range.clone()) else {
            if !range.is_empty() {
                debug_assert!(
                    false,
                    "You're trying to pull a range of {range:?} from a vec of len {} - something's wrong with partitioning probably, but that's all I can give you.",
                    from.len()
                );
            }
            return;
        };

        std::iter::Extend::extend(self, items.iter().cloned())
    }

    fn finish(self) -> Option<Self::Output> {
        self.is_empty().not().then_some(self)
    }
}

impl<'a> Builder<InternedStr<'a>> for DictionaryBuffer {
    // Since we're building this to send over protobuf, and protobuf needs optionals for all custom
    // structs. So this'll always be Some but it has to be optional for the interface.
    type Output = Option<InternedStrings>;
    type ExtendableFrom = InternedStrings;

    fn push<'b>(&mut self, item: InternedStr<'b>) {
        self.push_str(item.0)
    }

    fn extend_from_range(&mut self, range: Range<usize>, from: &Self::ExtendableFrom) {
        self.encoded.reserve(range.len());

        for i in range {
            // I feel like we can make this smarter somehow, but it would (probably) be very
            // complicated and isn't worth it right now. Something with calculating ranges of values
            // that need to be pulled over, then pushing ranges instead of individual values.
            let Some(item) = from.get(i) else {
                debug_assert!(
                    false,
                    "You're trying to get item {i} from an `InternedStrings` that only has {} elements - something's probably off with partitioning",
                    from.len()
                );
                continue;
            };

            self.push(item);
        }
    }

    fn finish(self) -> Option<Self::Output> {
        Self::finish(self).map(Some)
    }
}

impl<'a> Builder<PackedStr<'a>> for StringBuffer {
    // Option for the same reason as DictionaryBuffer::Output
    type Output = Option<PackedStrings>;
    type ExtendableFrom = PackedStrings;

    fn push<'b>(&mut self, item: PackedStr<'b>) {
        self.push_str(item.0)
    }

    fn extend_from_range(&mut self, range: Range<usize>, from: &Self::ExtendableFrom) {
        if range.is_empty() {
            return;
        }

        // need to get to ..=range.end instead of just ..range.end so that we get the final offset as well
        let fixed_range = range.start..=range.end;
        let Some(self_offsets_slice @ [start_offset, .., end_offset]) =
            from.offsets.get(fixed_range.clone())
        else {
            debug_assert!(
                false,
                "You're trying to get at least 2 elements with a range of {fixed_range:?} from a `PackedStrings` that only has {} offsets - something's probably off with partitioning",
                from.offsets.len()
            );
            return;
        };

        let values_str = from
            .values
            .split_at(*start_offset as usize)
            .1
            .split_at((end_offset - start_offset) as usize)
            .0;

        let buf_last_offset = self.0.values.len() as u32;

        self.0.values.push_str(values_str);
        // Need to skip the first element from this since it's already represented by the current
        // end of the offsets already stored
        self.0.offsets.extend(
            self_offsets_slice
                .iter()
                .skip(1)
                .map(|i| (i - start_offset) + buf_last_offset),
        );
    }

    fn finish(self) -> Option<Self::Output> {
        Self::finish(self).map(Some)
    }
}

/// This is the core partitioning function that pulls values from `original`, based on the ranges in
/// `ranges`, and produces a [`Builder::Output`] from them.
///
/// `original` is generally a slice-like value, e.g. &[i64], but can be something more complex, like
/// `&PackedStrings`. In this fn, we iterate over each range in `ranges`, pull that range out of the
/// null mask and original structure, and push it to a new structure that we're creating. We then
/// return the new structure and null mask.
pub fn slice_values<I, B: Builder<I>>(
    ranges: &[Range<usize>],
    original: &B::ExtendableFrom,
    orig_null_mask: &[u8],
) -> Option<(B::Output, Vec<u8>)> {
    let mut null_mask = NullMaskBuilder::default();
    let mut value_buf = B::default();

    for Range { start, end } in ranges {
        if orig_null_mask.is_empty() {
            value_buf.extend_from_range(*start..*end, original);
        } else {
            let max_bytes_in_null_mask = orig_null_mask.len();

            // We're ok with just getting everything we can get since there are other parts of this
            // system that remove all irrelevant trailing bytes off the null mask. For example, if
            // the last 16 elements in this column are non-null, we can remove the last 2 bytes off
            // the null mask and just know that everything past the end of the null mask is valid.
            let slice_end = end.div_ceil(8).min(max_bytes_in_null_mask);
            let relevant_null_mask_bytes = &orig_null_mask[..slice_end];

            // Ok. So say our null mask is [0b00000010]. This means that the first item is valid,
            // the second item is not, and the rest after that are valid.
            // And say we want to get range 10..12, right?
            // So we should look at this and say 'well, in the null mask that we can see, there is 1
            // invalid/null item. That means that we should just shift over every item in the range
            // by 1, right?
            //
            // Well, what if the null mask is [0b00000010, 0b00001000]? Then we have a null right in
            // the middle of our intended slice.
            // So we should slice up to the start of our range or the end of the null mask, whichever
            // comes first, and count the number of nulls in that. Then we should slice the null
            // mask by our wanted range, and count the number of nulls in that. Then decrement both
            // the start and end of the range by how many nulls exist before, and decrement the end
            // of the range by how many nulls exist inside.
            let relevant_bits = relevant_null_mask_bytes.view_bits::<Lsb0>();

            let adjusted_start = *start.min(&relevant_bits.len());
            let nulls_before_slice = relevant_bits[..adjusted_start].count_ones();

            let adjusted_end = *end.min(&relevant_bits.len());

            // This mask slice may be &[] if we're looking at a range outside of the null mask.
            // That's fine, our resulting null mask will just be empty, and that's exactly what we
            // want.
            let mask_slice = &relevant_bits[adjusted_start..adjusted_end];
            let nulls_in_slice = mask_slice.count_ones();

            for is_null_bit in mask_slice {
                match *is_null_bit {
                    true => null_mask.push(Value::Null),
                    false => null_mask.push(Value::NonNull),
                }
            }

            let real_start = start - nulls_before_slice;
            let real_end = end - (nulls_before_slice + nulls_in_slice);

            let unpushed_nonnull_mask_bits = (real_end - real_start) - mask_slice.count_zeros();
            for _ in 0..unpushed_nonnull_mask_bits {
                null_mask.push(Value::NonNull);
            }

            value_buf.extend_from_range(real_start..real_end, original);
        }
    }

    value_buf.finish().map(|v| (v, null_mask.finish()))
}

#[cfg(test)]
mod tests {
    #![expect(clippy::single_range_in_vec_init)]
    use core::{fmt::Debug, ops::Range};

    use crate::{
        builder::{Builder, DictionaryBuffer, StringBuffer, slice_values},
        values::{InternedStr, PackedStr},
    };

    use influxdb_line_protocol::test_helpers::{
        arbitrary_friendly_string, arbitrary_nonempty_range_within,
    };
    use pretty_assertions::assert_eq;
    use proptest::prelude::*;

    fn create_and_slice<I, E, B>(values: &[I], ranges: &[Range<usize>])
    where
        I: Clone,
        B: Builder<I, Output = Option<E>, ExtendableFrom = E>,
        E: Debug + PartialEq,
    {
        let mut orig_builder = B::default();
        for v in values {
            orig_builder.push(v.clone());
        }
        let orig = orig_builder.finish().unwrap().unwrap();

        let mut builder = B::default();
        for range in ranges {
            builder.extend_from_range(range.clone(), &orig);
        }
        let got = builder.finish().unwrap().unwrap();

        let mut expected_builder = B::default();
        for range in ranges {
            for s in &values[range.clone()] {
                expected_builder.push(s.clone());
            }
        }
        let expected_values = expected_builder.finish().unwrap().unwrap();

        assert_eq!(got, expected_values);
    }

    proptest! {
        #[test]
        fn dictionary_and_packed_str_buffer_slicing(
            (ranges, values) in proptest::collection::vec(arbitrary_friendly_string(true), 1..30)
                .prop_flat_map(|v: Vec<String>| (
                    proptest::collection::vec(arbitrary_nonempty_range_within(v.len()), 1..10),
                    Just(v)
                ))
        ) {
            let interned_strs = values.iter().map(|s: &String| InternedStr(s.as_str())).collect::<Vec<_>>();
            create_and_slice::<_, _, DictionaryBuffer>(&interned_strs, &ranges);

            let packed_strs = values.iter().map(|s: &String| PackedStr(s.as_str())).collect::<Vec<_>>();
            create_and_slice::<_, _, StringBuffer>(&packed_strs, &ranges);
        }
    }

    #[test]
    fn slice_values_works() {
        let (values, null_mask) = slice_values::<_, Vec<_>>(
            &[1..2, 4..7],
            &["q", "w", "e", "r", "t", "y", "u", "i", "o", "p"],
            &[0b01010101],
        )
        .unwrap();
        // we take [1, 4, 5, 6]. The null bits are [0, 1, 0, 1]. So the rest of the bits should be 0.

        assert_eq!(values, &["q", "e"]);
        assert_eq!(null_mask, &[0b00001010]);

        let (values, null_mask) = slice_values::<_, Vec<_>>(
            &[0..11],
            &["q", "w", "e", "r", "t", "y", "u", "i", "o", "p"],
            &[0b00000010],
        )
        .unwrap();

        assert_eq!(values, &["q", "w", "e", "r", "t", "y", "u", "i", "o", "p"]);
        assert_eq!(null_mask, &[0b00000010]);

        let (values, null_mask) =
            slice_values::<_, Vec<_>>(&[5..7], &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9], &[]).unwrap();

        assert_eq!(values, &[5, 6]);
        assert_eq!(null_mask, &[0u8; 0]);

        // It should work fine even if the ranges overlap
        let (values, null_mask) = slice_values::<_, Vec<_>>(
            &[3..7, 4..7, 1..3],
            &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            &[0b00010001, 0b0010001],
        )
        .unwrap();

        assert_eq!(values, &[2, 3, 4, 3, 4, 0, 1]);
        assert_eq!(null_mask, &[0b00010010]);
    }

    #[test]
    fn slice_values_with_temporary_nonnulls() {
        // We need to make sure that if we push some values that are off the end of the value mask,
        // then push more values that contain a null, the unset bits in the null mask will still be
        // pushed and show in the end
        let (values, null_mask) =
            slice_values::<_, Vec<_>>(&[1..17, 12..13], &[0, 1, 2], &[0b11111111, 0b01111111])
                .unwrap();

        assert_eq!(values, &[0, 1]);
        assert_eq!(null_mask, &[0b11111111, 0b00111111, 0b1]);
    }
}
