//! A fuzz test of the [`mutable_batch::Writer`] interface:
//!
//! - column writes - `write_i64`, `write_tag`, etc...
//! - batch writes - `write_batch`
//! - batch writes with ranges - `write_batch_ranges`
//!
//! Verifies that the rows and statistics are as expected after a number of interleaved writes

use arrow::{
    array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampNanosecondArray,
        UInt64Array,
    },
    record_batch::RecordBatch,
};
use arrow_util::bitset::BitSet;
use data_types::{IsNan, PartitionTemplate, StatValues, Statistics, TemplatePart};
use hashbrown::HashSet;
use mutable_batch::{writer::Writer, MutableBatch, PartitionWrite, WritePayload};
use rand::prelude::*;
use schema::Projection;
use std::{collections::BTreeMap, num::NonZeroU64, ops::Range, sync::Arc};

fn make_rng() -> StdRng {
    let seed = rand::rngs::OsRng::default().next_u64();
    println!("Seed: {seed}");
    StdRng::seed_from_u64(seed)
}

/// A random unicode string of up to 20 codepoints
fn random_string(rng: &mut StdRng) -> String {
    let len = (rng.next_u32() % 64) as usize;
    rng.sample_iter::<char, _>(rand::distributions::Standard)
        .take(len)
        .collect()
}

fn random_bool(rng: &mut StdRng) -> bool {
    rng.sample(rand::distributions::Standard)
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

fn compute_stats<T: PartialOrd + IsNan + ToOwned<Owned = T>>(data: &[Option<T>]) -> StatValues<T> {
    let mut stats = StatValues::new_empty();
    for d in data {
        match d {
            Some(v) => stats.update(v),
            None => stats.update_for_nulls(1),
        }
    }
    stats
}

impl Expected {
    /// Returns a filtered version of `self` based on the provided `ranges`
    fn filter(self, ranges: &[Range<usize>]) -> Expected {
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
    fn concat(&mut self, other: &Expected) {
        self.time_expected.extend_from_slice(&other.time_expected);
        self.tag_expected.extend_from_slice(&other.tag_expected);
        self.string_expected
            .extend_from_slice(&other.string_expected);
        self.bool_expected.extend_from_slice(&other.bool_expected);
        self.i64_expected.extend_from_slice(&other.i64_expected);
        self.u64_expected.extend_from_slice(&other.u64_expected);
        self.f64_expected.extend_from_slice(&other.f64_expected);
    }

    /// Reports the statistics indexed by column
    fn stats(&self) -> BTreeMap<String, Statistics> {
        let mut stats = BTreeMap::new();
        stats.insert(
            "b1".to_string(),
            Statistics::Bool(compute_stats(&self.bool_expected)),
        );
        stats.insert(
            "f1".to_string(),
            Statistics::F64(compute_stats(&self.f64_expected)),
        );
        stats.insert(
            "i1".to_string(),
            Statistics::I64(compute_stats(&self.i64_expected)),
        );
        stats.insert(
            "s1".to_string(),
            Statistics::String(compute_stats(&self.string_expected)),
        );
        stats.insert(
            "u1".to_string(),
            Statistics::U64(compute_stats(&self.u64_expected)),
        );

        let mut tag_stats = StatValues::new_empty();
        let mut tags = HashSet::new();
        for tag in &self.tag_expected {
            match tag {
                Some(v) => {
                    tags.insert(v.as_str());
                    tag_stats.update(v);
                }
                None => tag_stats.update_for_nulls(1),
            }
        }

        // Null counts as a distinct value
        match tag_stats.null_count {
            None => unreachable!("mutable batch keeps null counts"),
            Some(0) => tag_stats.distinct_count = NonZeroU64::new(tags.len() as u64),
            Some(_) => tag_stats.distinct_count = NonZeroU64::new(tags.len() as u64 + 1),
        }

        stats.insert("t1".to_string(), Statistics::String(tag_stats));

        let mut time_stats = StatValues::new_empty();
        self.time_expected.iter().for_each(|x| time_stats.update(x));
        stats.insert("time".to_string(), Statistics::I64(time_stats));

        stats
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
                .extend(std::iter::repeat(None).take(len as usize)),
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
                    .extend(std::iter::repeat(None).take(len as usize)),
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
            .extend(std::iter::repeat(None).take(len as usize)),
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
            .extend(std::iter::repeat(None).take(len as usize)),
    }

    match maybe_array(rng, len, |rng| rng.next_u64()) {
        Some(array) => {
            expected.u64_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_u64("u1", Some(mask.bytes()), array.iter().filter_map(|x| *x))
                .unwrap();
        }
        None => expected
            .u64_expected
            .extend(std::iter::repeat(None).take(len as usize)),
    }

    match maybe_array(rng, len, |rng| rng.next_u64() as i64) {
        Some(array) => {
            expected.i64_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_i64("i1", Some(mask.bytes()), array.iter().filter_map(|x| *x))
                .unwrap();
        }
        None => expected
            .i64_expected
            .extend(std::iter::repeat(None).take(len as usize)),
    }

    match maybe_array(rng, len, |rng| f64::from_bits(rng.next_u64())) {
        Some(array) => {
            expected.f64_expected.extend(array.iter().cloned());
            let mask = compute_mask(&array);
            writer
                .write_f64("f1", Some(mask.bytes()), array.iter().filter_map(|x| *x))
                .unwrap();
        }
        None => expected
            .f64_expected
            .extend(std::iter::repeat(None).take(len as usize)),
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

    let actual = batch.to_arrow(Projection::All).unwrap();

    assert_eq!(
        arrow_util::display::pretty_format_batches(&[actual]).unwrap(),
        arrow_util::display::pretty_format_batches(&[expected.batch()]).unwrap()
    );

    let actual_statistics: BTreeMap<String, Statistics> = batch
        .columns()
        .map(|(name, col)| (name.clone(), col.stats()))
        .collect();
    let expected_statistics = expected.stats();

    assert_eq!(actual_statistics, expected_statistics);
}

#[test]
fn test_partition_write() {
    let mut rng = make_rng();
    let mut batch = MutableBatch::new();
    let expected = extend_batch(&mut rng, &mut batch);

    let w = PartitionWrite::new(&batch);
    assert_eq!(w.rows().get(), expected.tag_expected.len());

    let verify_write = |write: &PartitionWrite<'_>| {
        // Verify that the time and row statistics computed by the PartitionWrite
        // match what actually gets written to a MutableBatch
        let mut temp = MutableBatch::new();
        write.write_to_batch(&mut temp).unwrap();

        let stats = match temp.column("time").unwrap().stats() {
            Statistics::I64(stats) => stats,
            _ => unreachable!(),
        };

        assert_eq!(write.min_timestamp(), stats.min.unwrap());
        assert_eq!(write.max_timestamp(), stats.max.unwrap());
        assert_eq!(write.rows().get() as u64, stats.total_count);
    };

    let partitioned = PartitionWrite::partition(
        "table",
        &batch,
        &PartitionTemplate {
            parts: vec![TemplatePart::Column("b1".to_string())],
        },
    );

    for (_, write) in &partitioned {
        verify_write(write);

        match write.filter(|x| x & 1 == 0) {
            Some(filtered) => verify_write(&filtered),
            None => continue,
        }
    }
}
