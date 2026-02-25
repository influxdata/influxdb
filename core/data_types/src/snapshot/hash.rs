//! A primitive hash table supporting linear probing

use bytes::Bytes;
use generated_types::influxdata::iox::catalog_cache::v1 as generated;
use siphasher::sip::SipHasher24;

use snafu::{Snafu, ensure};

/// Error for [`HashBuckets`]
#[derive(Debug, Snafu)]
#[expect(missing_docs, missing_copy_implementations)]
pub enum Error {
    #[snafu(display("Bucket length not a power of two"))]
    BucketsNotPower,
    #[snafu(display("Unrecognized hash function"))]
    UnrecognizedHash,
}

/// Result for [`HashBuckets`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A primitive hash table supporting [linear probing]
///
/// [linear probing](https://en.wikipedia.org/wiki/Linear_probing)
#[derive(Debug, Clone)]
pub struct HashBuckets {
    /// The mask to yield index in `buckets` from a u64 hash
    mask: usize,
    /// A sequence of u32 encoding the value index + 1, or 0 if empty
    buckets: Bytes,
    /// The hash function to use
    hash: SipHasher24,
}

impl HashBuckets {
    /// Performs a lookup of `value`
    pub fn lookup(&self, value: &[u8]) -> HashProbe<'_> {
        self.lookup_raw(self.hash.hash(value))
    }

    fn lookup_raw(&self, hash: u64) -> HashProbe<'_> {
        let idx = (hash as usize) & self.mask;
        HashProbe {
            idx,
            buckets: self,
            mask: self.mask as _,
        }
    }
}

impl TryFrom<generated::HashBuckets> for HashBuckets {
    type Error = Error;

    fn try_from(value: generated::HashBuckets) -> std::result::Result<Self, Self::Error> {
        let buckets_len = value.buckets.len();
        ensure!(buckets_len.count_ones() == 1, BucketsNotPowerSnafu);
        let mask = buckets_len.wrapping_sub(1) ^ 3;
        match value.hash_function {
            Some(generated::hash_buckets::HashFunction::SipHash24(s)) => Ok(Self {
                mask,
                buckets: value.buckets,
                hash: SipHasher24::new_with_keys(s.key0, s.key1),
            }),
            _ => Err(Error::UnrecognizedHash),
        }
    }
}

impl From<HashBuckets> for generated::HashBuckets {
    fn from(value: HashBuckets) -> Self {
        let (key0, key1) = value.hash.keys();
        Self {
            buckets: value.buckets,
            hash_function: Some(generated::hash_buckets::HashFunction::SipHash24(
                generated::SipHash24 { key0, key1 },
            )),
        }
    }
}

/// Yields the indices to probe for equality
#[derive(Debug)]
pub struct HashProbe<'a> {
    buckets: &'a HashBuckets,
    idx: usize,
    mask: usize,
}

impl Iterator for HashProbe<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let slice = self.buckets.buckets.get(self.idx..self.idx + 4)?;
        let entry = u32::from_le_bytes(slice.try_into().unwrap());
        self.idx = (self.idx + 4) & self.mask;

        // Empty entries are encoded as 0
        Some(entry.checked_sub(1)? as usize)
    }
}

/// An encoder for [`HashBuckets`]
#[derive(Debug)]
pub struct HashBucketsEncoder {
    mask: usize,
    buckets: Vec<u8>,
    hash: SipHasher24,
    len: u32,
    capacity: u32,
}

impl HashBucketsEncoder {
    /// Create a new [`HashBucketsEncoder`]
    ///
    /// # Panics
    ///
    /// Panics if capacity >= u32::MAX
    pub fn new(capacity: usize) -> Self {
        assert!(capacity < u32::MAX as usize);

        let buckets_len = (capacity * 2).next_power_of_two() * 4;
        let mask = buckets_len.wrapping_sub(1) ^ 3;
        Self {
            mask,
            len: 0,
            capacity: capacity as u32,
            buckets: vec![0; buckets_len],
            // Note: this uses keys (0, 0)
            hash: SipHasher24::new(),
        }
    }

    /// Append a new (key, value) pair
    ///
    /// # Panics
    ///
    /// Panics if this would exceed the capacity provided to new
    pub fn push(&mut self, key: &[u8], value: u32) {
        self.push_raw(self.hash.hash(key), value);
    }

    /// Append a new value by hash, returning the bucket index
    fn push_raw(&mut self, hash: u64, value: u32) -> usize {
        assert_ne!(self.len, self.capacity, "hash table capacity exceeded");
        self.len += 1;
        // Note: empty entries are encoded as 0
        let entry = value + 1;
        let mut idx = (hash as usize) & self.mask;
        loop {
            let s = &mut self.buckets[idx..idx + 4];
            let s: &mut [u8; 4] = s.try_into().unwrap();
            if s.iter().all(|x| *x == 0) {
                *s = entry.to_le_bytes();
                return idx / 4;
            }
            idx = (idx + 4) & self.mask;
        }
    }

    /// Construct the output [`HashBuckets`]
    pub fn finish(self) -> HashBuckets {
        HashBuckets {
            mask: self.mask,
            hash: self.hash,
            buckets: self.buckets.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use rand::{Rng, rng};

    #[test]
    fn test_collision() {
        let mut builder = HashBucketsEncoder::new(6);

        let mut counter = 0;
        assert_eq!(builder.push_raw(14, counter), 3);
        counter += 1;
        assert_eq!(builder.push_raw(297, counter), 10);
        counter += 1;
        assert_eq!(builder.push_raw(43, counter), 11); // Hashes to occupied bucket 10
        counter += 1;
        assert_eq!(builder.push_raw(60, counter), 15);
        counter += 1;
        assert_eq!(builder.push_raw(124, counter), 0); // Hashes to occupied bucket 15
        counter += 1;
        assert_eq!(builder.push_raw(0, counter), 1); // Hashes to occupied bucket 0

        let buckets = builder.finish();

        let l = buckets.lookup_raw(14).collect::<Vec<_>>();
        assert_eq!(l, vec![0]);

        let l = buckets.lookup_raw(297).collect::<Vec<_>>();
        assert_eq!(l, vec![1, 2]);

        let l = buckets.lookup_raw(43).collect::<Vec<_>>();
        assert_eq!(l, vec![1, 2]);

        let l = buckets.lookup_raw(60).collect::<Vec<_>>();
        assert_eq!(l, vec![3, 4, 5]);

        let l = buckets.lookup_raw(0).collect::<Vec<_>>();
        assert_eq!(l, vec![4, 5]);
    }

    #[test]
    fn test_basic() {
        let data = ["a", "", "bongos", "cupcakes", "bananas"];
        let mut builder = HashBucketsEncoder::new(data.len());
        for (index, str) in data.iter().enumerate() {
            builder.push(str.as_bytes(), index as u32);
        }
        let buckets = builder.finish();

        let contains = |s: &str| -> bool { buckets.lookup(s.as_bytes()).any(|idx| data[idx] == s) };

        assert!(contains("a"));
        assert!(contains(""));
        assert!(contains("bongos"));
        assert!(contains("cupcakes"));
        assert!(contains("bananas"));
        assert!(!contains("windows"));
    }

    // test "sparse" indices (ex. when soft-deleted entries are skipped)
    #[test]
    fn test_sparse() {
        let data = ["a", "", "bongos", "cupcakes", "bananas"];
        let mut builder = HashBucketsEncoder::new(data.len() / 2 + data.len() % 2);
        for (index, str) in data.iter().enumerate() {
            if index % 2 == 0 {
                builder.push(str.as_bytes(), index as u32);
            }
        }
        let buckets = builder.finish();
        let contains = |s: &str| -> bool { buckets.lookup(s.as_bytes()).any(|idx| data[idx] == s) };

        assert!(contains("a"));
        assert!(!contains(""));
        assert!(contains("bongos"));
        assert!(!contains("cupcakes"));
        assert!(contains("bananas"));
        assert!(!contains("windows"));
    }

    // test max length/capacity behavior
    #[test]
    #[should_panic(
        expected = "assertion `left != right` failed: hash table capacity exceeded\n  left: 3\n right: 3"
    )]
    fn test_capacity() {
        let mut rng = rng();
        let data: Vec<Vec<u8>> = (0..6)
            .map(|_| (0..rng.random_range(0..3)).map(|_| rng.random()).collect())
            .collect();
        // create hash table with capacity less than input data
        let mut builder = HashBucketsEncoder::new(data.len() / 2);
        data.iter()
            .enumerate()
            .for_each(|(i, x)| builder.push(x.as_ref(), i as u32));
    }

    // Edge case: hash bucket representation cannot encode u32::MAX
    #[test]
    #[should_panic(expected = "attempt to add with overflow")]
    fn test_max_bucket_entry() {
        let mut builder = HashBucketsEncoder::new(1);
        builder.push("".as_bytes(), u32::MAX);
        builder.finish();
    }

    #[test]
    fn test_deterministic() {
        // It is important that HashBuckets is deterministic for ETag matching to work
        let mut rng = rng();
        let data: Vec<Vec<u8>> = (0..20)
            .map(|_| (0..rng.random_range(0..20)).map(|_| rng.random()).collect())
            .collect();

        let build = || {
            let mut builder = HashBucketsEncoder::new(data.len());
            data.iter()
                .enumerate()
                .for_each(|(i, x)| builder.push(x.as_ref(), i as u32));
            generated::HashBuckets::from(builder.finish()).encode_to_vec()
        };

        assert_eq!(build(), build());
    }
}
