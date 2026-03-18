//! A fuzz test of the [`mutable_batch::Writer`] interface:
//!
//! - column writes - `write_i64`, `write_tag`, etc...
//! - batch writes - `write_batch`
//! - batch writes with ranges - `write_batch_ranges`
//!
//! Verifies that the rows and statistics are as expected after a number of interleaved writes

// Tests and benchmarks don't use all the crate dependencies and that's all right.
#![expect(unused_crate_dependencies)]

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt64Array,
    },
    record_batch::RecordBatch,
};
use arrow_util::bitset::BitSet;
use data_types::partition_template::{TemplatePart, test_table_partition_override};
use mutable_batch::{MutableBatch, WritePayload, writer::Writer};
use partition::PartitionWrite;
use pretty_assertions::assert_eq;
use rand::TryRngCore;
use rand::prelude::*;
use schema::Projection;
use std::{ops::Range, sync::Arc};

fn make_rng() -> StdRng {
    let seed = rand::rngs::OsRng
        .try_next_u64()
        .expect("unable to get random seed");
    println!("Seed: {seed}");
    StdRng::seed_from_u64(seed)
}

/// A random unicode string of up to 20 codepoints
fn random_string(rng: &mut StdRng) -> String {
    let len = (rng.next_u32() % 64) as usize;
    rng.sample_iter::<char, _>(rand::distr::StandardUniform)
        .take(len)
        .collect()
}

fn random_bool(rng: &mut StdRng) -> bool {
    rng.sample(rand::distr::StandardUniform)
}

/// Randomly may return an array containing randomly generated, nullable data
fn maybe_array<T, F>(rng: &mut StdRng, len: u32, generator: F) -> Option<Vec<Option<T>>>
where
    F: Fn(&mut StdRng) -> T,
{
    match random_bool(rng) {
        true => None,
        false => Some(
            (0..len)
                .map(|_| match random_bool(rng) {
                    true => Some(generator(rng)),
                    false => None,
                })
                .collect::<Vec<_>>(),
        ),
    }
}

fn compute_mask<T>(array: &[Option<T>]) -> BitSet {
    let mut bitset = BitSet::new();
    bitset.append_unset(array.len());
    for (idx, v) in array.iter().enumerate() {
        if v.is_some() {
            bitset.set(idx)
        }
    }
    bitset
}

/// The expected data that was written
#[derive(Debug, Default)]
struct Expected {
    time_expected: Vec<i64>,
    tag_expected: Vec<Option<String>>,
    string_expected: Vec<Option<String>>,
    bool_expected: Vec<Option<bool>>,
    i64_expected: Vec<Option<i64>>,
    u64_expected: Vec<Option<u64>>,
    f64_expected: Vec<Option<f64>>,
}

fn filter_vec<T: Clone>(ranges: &[Range<usize>], src: &[T]) -> Vec<T> {
    ranges
        .iter()
        .flat_map(|r| r.clone())
        .map(|x| src[x].clone())
        .collect()
}

impl Expected {
    /// Returns a filtered version of `self` based on the provided `ranges`
    fn filter(self, ranges: &[Range<usize>]) -> Self {
        Self {
            time_expected: filter_vec(ranges, &self.time_expected),
            tag_expected: filter_vec(ranges, &self.tag_expected),
            string_expected: filter_vec(ranges, &self.string_expected),
            bool_expected: filter_vec(ranges, &self.bool_expected),
            i64_expected: filter_vec(ranges, &self.i64_expected),
            u64_expected: filter_vec(ranges, &self.u64_expected),
            f64_expected: filter_vec(ranges, &self.f64_expected),
        }
    }

    /// Extends `self` with the writes from `other`
    fn concat(&mut self, other: &Self) {
        self.time_expected.extend_from_slice(&other.time_expected);
        self.tag_expected.extend_from_slice(&other.tag_expected);
        self.string_expected
            .extend_from_slice(&other.string_expected);
        self.bool_expected.extend_from_slice(&other.bool_expected);
        self.i64_expected.extend_from_slice(&other.i64_expected);
        self.u64_expected.extend_from_slice(&other.u64_expected);
        self.f64_expected.extend_from_slice(&other.f64_expected);
    }

    /// Converts this to a [`RecordBatch`]
    fn batch(&self) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "b1",
                Arc::new(BooleanArray::from_iter(self.bool_expected.iter())) as ArrayRef,
            ),
            (
                "f1",
                Arc::new(Float64Array::from_iter(self.f64_expected.iter())) as ArrayRef,
            ),
            (
                "i1",
                Arc::new(Int64Array::from_iter(self.i64_expected.iter())) as ArrayRef,
            ),
            (
                "s1",
                Arc::new(StringArray::from_iter(self.string_expected.iter())) as ArrayRef,
            ),
            (
                "t1",
                Arc::new(StringArray::from_iter(self.tag_expected.iter())) as ArrayRef,
            ),
            (
                "time",
                Arc::new(TimestampNanosecondArray::from_iter_values(
                    self.time_expected.iter().cloned(),
                )) as ArrayRef,
            ),
            (
                "u1",
                Arc::new(UInt64Array::from_iter(self.u64_expected.iter())) as ArrayRef,
            ),
        ])
        .unwrap()
    }
}

/// Extends the provided batch with random content, returning a summary of what was written
fn extend_batch(rng: &mut StdRng, batch: &mut MutableBatch) -> Expected {
    let len = rng.next_u32() % 128 + 1;
    let mut expected = Expected::default();

    let mut writer = Writer::new(batch, len as usize);

    let time: Vec<_> = std::iter::from_fn(|| Some(rng.next_u32() as i64))
        .take(len as usize)
        .collect();

    writer.write_time("time", time.iter().cloned()).unwrap();
    expected.time_expected.extend_from_slice(&time);

    match random_bool(rng) {
        true => match maybe_array(rng, len, random_string) {
            Some(array) => {
                expected.tag_expected.extend(array.iter().cloned());
                let mask = compute_mask(&array);
                writer
                    .write_tag(
                        "t1",
                        Some(mask.bytes()),
                        array.iter().filter_map(|x| x.as_deref()),
                    )
                    .unwrap();
            }
            None => expected
                .tag_expected
                .extend(std::iter::repeat_n(None, len as usize)),
        },
        false => {
            let values_len = rng.next_u32() % 18 + 1;
            let values: Vec<_> = std::iter::from_fn(|| Some(random_string(rng)))
                .take(values_len as usize)
                .collect();

            match maybe_array(rng, len, |rng| (rng.next_u32() % values_len) as usize) {
                Some(array) => {
                    expected
                        .tag_expected
                        .extend(array.iter().map(|x| Some(values[(*x)?].clone())));
                    let mask = compute_mask(&array);
                    writer
                        .write_tag_dict(
                            "t1",
                            Some(mask.bytes()),
                            array.iter().filter_map(|x| *x),
                            values.iter().map(|x| x.as_str()),
                        )
                        .unwrap();
                }
                None => expected
                    .tag_expected
                    .extend(std::iter::repeat_n(None, len as usize)),
            }
        }
    }

    match maybe_array(rng, len, random_string) {
        Some(array) => {
            expected.string_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_string(
                    "s1",
                    Some(mask.bytes()),
                    array.iter().filter_map(|x| x.as_deref()),
                )
                .unwrap();
        }
        None => expected
            .string_expected
            .extend(std::iter::repeat_n(None, len as usize)),
    }

    match maybe_array(rng, len, random_bool) {
        Some(array) => {
            expected.bool_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_bool("b1", Some(mask.bytes()), array.iter().filter_map(|x| *x))
                .unwrap();
        }
        None => expected
            .bool_expected
            .extend(std::iter::repeat_n(None, len as usize)),
    }

    match maybe_array(rng, len, |rng| rng.next_u64()) {
        Some(array) => {
            expected.u64_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_u64("u1", mask.bytes(), array.iter().filter_map(|x| *x))
                .unwrap();
        }
        None => expected
            .u64_expected
            .extend(std::iter::repeat_n(None, len as usize)),
    }

    match maybe_array(rng, len, |rng| rng.next_u64() as i64) {
        Some(array) => {
            expected.i64_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_i64("i1", mask.bytes(), array.iter().filter_map(|x| *x))
                .unwrap();
        }
        None => expected
            .i64_expected
            .extend(std::iter::repeat_n(None, len as usize)),
    }

    match maybe_array(rng, len, |rng| f64::from_bits(rng.next_u64())) {
        Some(array) => {
            expected.f64_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_f64("f1", mask.bytes(), array.iter().filter_map(|x| *x))
                .unwrap();
        }
        None => expected
            .f64_expected
            .extend(std::iter::repeat_n(None, len as usize)),
    }

    writer.commit();
    expected
}

/// Returns random non-overlapping ranges in increasing order with a max of len
fn random_ranges(rng: &mut StdRng, len: usize) -> Vec<Range<usize>> {
    let mut start = rng.next_u64() as usize % len;

    let mut ret = vec![];
    while start < len {
        let end = (start + rng.next_u32() as usize % 32).min(len);
        ret.push(start..end);
        start = end + rng.next_u32() as usize % 32;
    }
    ret
}

#[test]
fn test_writer_fuzz() {
    let mut rng = make_rng();
    let mut batch = MutableBatch::new();
    let mut expected = Expected::default();

    // Perform some regular writes
    for _ in 0..20 {
        let ret = extend_batch(&mut rng, &mut batch);
        expected.concat(&ret);
    }

    // Test extend from
    for _ in 0..20 {
        let mut temp = MutableBatch::new();
        let ret = extend_batch(&mut rng, &mut temp);
        batch.extend_from(&temp).unwrap();
        expected.concat(&ret);
    }

    // Test extend from ranges
    for _ in 0..20 {
        let mut temp = MutableBatch::new();
        let ret = extend_batch(&mut rng, &mut temp);

        let ranges = random_ranges(&mut rng, temp.rows());
        batch.extend_from_ranges(&temp, &ranges).unwrap();

        expected.concat(&ret.filter(&ranges));
    }

    let actual = batch.clone().try_into_arrow(Projection::All).unwrap();

    assert_eq!(
        arrow_util::display::pretty_format_batches(&[actual]).unwrap(),
        arrow_util::display::pretty_format_batches(&[expected.batch()]).unwrap()
    );
}

#[test]
fn test_partition_write() {
    let mut rng = make_rng();
    let mut batch = MutableBatch::new();
    let expected = extend_batch(&mut rng, &mut batch);

    let w = PartitionWrite::new(&batch).unwrap();
    assert_eq!(w.rows().get(), expected.tag_expected.len());

    let verify_write = |write: &PartitionWrite<'_, MutableBatch>| {
        // Verify that the time and row statistics computed by the PartitionWrite
        // match what actually gets written to a MutableBatch
        let mut temp = MutableBatch::new();
        write.write_to_batch(&mut temp).unwrap();
    };

    let table_partition_template =
        test_table_partition_override(vec![TemplatePart::TagValue("t1")]);

    let partitioned = PartitionWrite::partition(&batch, &table_partition_template).unwrap();

    for (_, write) in &partitioned {
        verify_write(write);

        match write.filter(|x| x & 1 == 0) {
            Some(filtered) => verify_write(&filtered),
            None => continue,
        }
    }
}
