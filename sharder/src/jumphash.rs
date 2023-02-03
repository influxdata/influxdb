use super::Sharder;
use data_types::{DeletePredicate, NamespaceName};
use mutable_batch::MutableBatch;
use siphasher::sip::SipHasher13;
use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
    sync::Arc,
};

/// A [`JumpHash`] maps operations for a given table in a given namespace
/// consistently to the same shard, irrespective of the operation itself with
/// near perfect distribution.
///
/// Different instances of a [`JumpHash`] using the same seed key, and the same
/// set of shards (in the same order) will always map the same input table &
/// namespace to the same shard `T`.
///
/// For `N` shards, this type uses `O(N)` memory and `O(ln N)` lookup, utilising
/// Google's [jump hash] internally. Adding 1 additional shard causes
/// approximately `1/N` keys to be remapped.
///
/// [jump hash]: https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
#[derive(Debug)]
pub struct JumpHash<T> {
    hasher: SipHasher13,
    shards: Vec<T>,
}

impl<T> JumpHash<T> {
    /// Initialise a [`JumpHash`] that consistently maps keys to
    /// one of `shards`.
    ///
    /// # Correctness
    ///
    /// Changing the number of, or order of, the elements in `shards` when
    /// constructing two instances changes the mapping produced.
    ///
    /// # Panics
    ///
    /// This constructor panics if the number of elements in `shards` is 0.
    pub fn new(shards: impl IntoIterator<Item = T>) -> Self {
        // A randomly generated static siphash key to ensure all router
        // instances hash the same input to the same u64 sharding key.
        //
        // Generated with: xxd -i -l 16 /dev/urandom
        let key = [
            0x6d, 0x83, 0x93, 0x52, 0xa3, 0x7c, 0xe6, 0x02, 0xac, 0x01, 0x11, 0x94, 0x79, 0x0c,
            0x64, 0x42,
        ];

        let shards = shards.into_iter().collect::<Vec<_>>();
        assert!(!shards.is_empty(), "empty shard set given to sharder");

        Self {
            hasher: SipHasher13::new_with_key(&key),
            shards,
        }
    }

    /// Return a slice of all the shards this instance is configured with,
    pub fn shards(&self) -> &[T] {
        &self.shards
    }
}

impl<T> JumpHash<T> {
    /// Reinitialise [`Self`] with a new key.
    ///
    /// Re-keying [`Self`] will change the mapping of inputs to output instances
    /// of `T`.
    pub fn with_seed_key(self, key: &[u8; 16]) -> Self {
        let hasher = SipHasher13::new_with_key(key);
        Self { hasher, ..self }
    }

    /// Consistently hash `key` to a `T`.
    pub fn hash<H>(&self, key: H) -> &T
    where
        H: Hash,
    {
        let mut state = self.hasher;
        key.hash(&mut state);
        let mut key = state.finish();

        let mut b = -1;
        let mut j = 0;
        while j < self.shards.len() as i64 {
            b = j;
            key = key.wrapping_mul(2862933555777941757).wrapping_add(1);
            j = ((b.wrapping_add(1) as f64) * (((1u64 << 31) as f64) / (((key >> 33) + 1) as f64)))
                as i64
        }

        assert!(b >= 0);
        self.shards
            .get(b as usize)
            .expect("sharder mapped input to non-existant bucket")
    }

    /// Consistently hash a table and namespace to a `T`. For use in a situation where you don't
    /// have a payload.
    pub fn shard_for_query(&self, table: &str, namespace: &str) -> &T {
        // The derived hash impl for HashKey is hardened against prefix
        // collisions when combining the two fields.
        self.hash(&HashKey { table, namespace })
    }
}

#[derive(Hash)]
struct HashKey<'a> {
    table: &'a str,
    namespace: &'a str,
}

/// A [`JumpHash`] sharder mapping a [`MutableBatch`] reference according to the
/// namespace it is destined for.
///
/// This currently doesn't use any information about the payload, just encodes
/// that a MutableBatch will always be sharded to one `Arc<T>`.
impl<T> Sharder<MutableBatch> for JumpHash<Arc<T>>
where
    T: Debug + Send + Sync,
{
    type Item = Arc<T>;

    fn shard(
        &self,
        table: &str,
        namespace: &NamespaceName<'_>,
        _payload: &MutableBatch,
    ) -> Self::Item {
        // Because the MutableBatch is not (currently) used to derive the shard
        // destination, delegate to the "no payload" sharder.
        Self::shard(self, table, namespace, &())
    }
}

/// A [`JumpHash`] sharder mapping a [`DeletePredicate`] reference to all
/// shards unless a table is specified, in which case the table & namespace are
/// used to shard to the same destination as a write with the same table &
/// namespace would.
impl<T> Sharder<DeletePredicate> for JumpHash<Arc<T>>
where
    T: Debug + Send + Sync,
{
    type Item = Vec<Arc<T>>;

    fn shard(
        &self,
        table: &str,
        namespace: &NamespaceName<'_>,
        _payload: &DeletePredicate,
    ) -> Self::Item {
        // A delete that does not specify a table is mapped to all shards.
        if table.is_empty() {
            return self.shards.iter().map(Arc::clone).collect();
        }

        // A delete that specifies a table is mapped to the shard responsible
        // for this (namespace, table) tuple.
        vec![Arc::clone(self.hash(&HashKey {
            table,
            namespace: namespace.as_ref(),
        }))]
    }
}

impl<T> Sharder<()> for JumpHash<Arc<T>>
where
    T: Debug + Send + Sync,
{
    type Item = Arc<T>;

    fn shard(&self, table: &str, namespace: &NamespaceName<'_>, _payload: &()) -> Self::Item {
        Arc::clone(self.shard_for_query(table, namespace.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use data_types::TimestampRange;
    use hashbrown::HashMap;
    use std::iter;

    #[test]
    fn test_consistent_hashing() {
        const NUM_TESTS: usize = 10_000;
        const NUM_SHARDS: usize = 10;

        let hasher = JumpHash::new(0..NUM_SHARDS);

        // Create a HashMap<key, shard> to verify against.
        let mappings = (0..NUM_TESTS)
            .map(|v| {
                let shard = hasher.hash(v);
                (v, shard)
            })
            .collect::<HashMap<_, _>>();

        // Rehash all the same keys and validate they map to the same shard.
        //
        // The random iteration order of the hashmap asserts the shard output is
        // not a function of the order of the keys hashed.
        assert!(mappings
            .iter()
            .all(|(&key, &value)| hasher.hash(key) == value));

        // Reinitialise the hasher with the same (default) key
        let hasher = JumpHash::new(0..NUM_SHARDS);

        // And assert the mappings are the same
        assert!(mappings
            .iter()
            .all(|(&key, &value)| hasher.hash(key) == value));

        // Reinitialise the hasher with the a different key
        let hasher = JumpHash::new(0..NUM_SHARDS).with_seed_key(&[42; 16]);

        // And assert the mappings are the NOT all same (some may be the same)
        assert!(!mappings
            .iter()
            .all(|(&key, &value)| hasher.hash(key) == value));
    }

    #[test]
    fn test_sharder_impl() {
        let hasher = JumpHash::new((0..10_000).map(Arc::new));

        let a = hasher.shard(
            "table",
            &NamespaceName::try_from("namespace").unwrap(),
            &MutableBatch::default(),
        );
        let b = hasher.shard(
            "table",
            &NamespaceName::try_from("namespace2").unwrap(),
            &MutableBatch::default(),
        );
        assert_ne!(a, b);

        let a = hasher.shard(
            "table",
            &NamespaceName::try_from("namespace").unwrap(),
            &MutableBatch::default(),
        );
        let b = hasher.shard(
            "table2",
            &NamespaceName::try_from("namespace").unwrap(),
            &MutableBatch::default(),
        );
        assert_ne!(a, b);

        let mut batches = mutable_batch_lp::lines_to_batches("cpu a=1i", 42).unwrap();
        let batch = batches.remove("cpu").unwrap();

        // Assert payloads are ignored for this sharder
        let a = hasher.shard(
            "table",
            &NamespaceName::try_from("namespace").unwrap(),
            &MutableBatch::default(),
        );
        let b = hasher.shard(
            "table",
            &NamespaceName::try_from("namespace").unwrap(),
            &batch,
        );
        assert_eq!(a, b);
    }

    #[test]
    fn test_sharder_prefix_collision() {
        let hasher = JumpHash::new((0..10_000).map(Arc::new));
        let a = hasher.shard(
            "a",
            &NamespaceName::try_from("bc").unwrap(),
            &MutableBatch::default(),
        );
        let b = hasher.shard(
            "ab",
            &NamespaceName::try_from("c").unwrap(),
            &MutableBatch::default(),
        );
        assert_ne!(a, b);
    }

    // This test ensures hashing key K always maps to bucket B, even after
    // dependency updates, code changes, etc.
    //
    // It is not a problem if these mappings change so long as all the nodes in
    // the cluster are producing the same mapping of K->B. However, this would
    // not be the case during a rolling deployment where some nodes are using
    // one mapping, and new nodes using another.
    //
    // This test being updated necessitates a stop-the-world deployment (stop
    // all routers, deploy new hashing code on all routers, resume serving
    // traffic) to inconsistently routing of ops. Also prepare a roll-back
    // strategy would that accounts for this mapping change.
    #[test]
    fn test_key_bucket_fixture() {
        let hasher = JumpHash::new((0..1_000).map(Arc::new));
        let namespace = NamespaceName::try_from("bananas").unwrap();

        let mut batches = mutable_batch_lp::lines_to_batches("cpu a=1i", 42).unwrap();
        let batch = batches.remove("cpu").unwrap();

        assert_eq!(
            *hasher.shard("42", &namespace, &MutableBatch::default()),
            904
        );
        assert_eq!(*hasher.shard("42", &namespace, &()), 904);
        assert_eq!(
            *hasher.shard("4242", &namespace, &MutableBatch::default()),
            230
        );
        assert_eq!(*hasher.shard("4242", &namespace, &()), 230);
        assert_eq!(*hasher.shard("bananas", &namespace, &batch), 183);
        assert_eq!(*hasher.shard("bananas", &namespace, &()), 183);
    }

    #[test]
    fn test_distribution() {
        let hasher = JumpHash::new((0..100).map(Arc::new));
        let namespace = NamespaceName::try_from("bananas").unwrap();

        let mut mapping = HashMap::<_, usize>::new();

        for i in 0..10_000_000 {
            let bucket = hasher.shard(
                format!("{i}").as_str(),
                &namespace,
                &MutableBatch::default(),
            );
            *mapping.entry(bucket).or_default() += 1;
        }

        let (min, max) = mapping.values().fold((usize::MAX, 0), |acc, &v| {
            let (min, max) = acc;
            (min.min(v), max.max(v))
        });

        // Expect that the number of values of each bucket are all within ±0.05%
        // of the total 10M values
        assert!(max - min < 5000, "min: {min}, max: {max}");
    }

    #[test]
    fn test_delete_with_table() {
        let namespace = NamespaceName::try_from("bananas").unwrap();

        let hasher = JumpHash::new((0..10_000).map(Arc::new));

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        let batch = MutableBatch::default();

        for i in 0..100_usize {
            // A delete with a table should map to exactly one shard.
            let mut got = hasher.shard(i.to_string().as_str(), &namespace, &predicate);
            assert_eq!(got.len(), 1);
            let delete_shard = got.pop().unwrap();

            // And a write to the same table & namespace MUST map to the same shard.
            let write_shard = hasher.shard(i.to_string().as_str(), &namespace, &batch);
            assert_eq!(delete_shard, write_shard);
        }
    }

    #[test]
    fn test_delete_no_table_shards_to_all() {
        let namespace = NamespaceName::try_from("bananas").unwrap();

        let shards = (0..10_000).map(Arc::new).collect::<Vec<_>>();
        let hasher = JumpHash::new(shards.clone());

        let predicate = DeletePredicate {
            range: TimestampRange::new(1, 2),
            exprs: vec![],
        };

        let got = hasher.shard("", &namespace, &predicate);

        assert_eq!(got, shards);
    }

    #[test]
    #[should_panic = "empty shard set given to sharder"]
    fn no_shards() {
        let shards: iter::Empty<i32> = iter::empty();
        JumpHash::new(shards);
    }
}
