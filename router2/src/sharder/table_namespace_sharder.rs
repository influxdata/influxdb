use std::{
    fmt::Debug,
    hash::{Hash, Hasher},
};

use data_types::DatabaseName;
use siphasher::sip::SipHasher13;

use super::Sharder;

/// A [`TableNamespaceSharder`] maps operations for a given table in a given
/// namespace consistently to the same shard, irrespective of the operation
/// itself with near perfect distribution.
///
/// Different instances of a [`TableNamespaceSharder`] using the same seed key,
/// and the same set of shards (in the same order) will always map the same
/// input table & namespace to the same shard `T`.
///
/// For `N` shards, this type uses `O(N)` memory and `O(ln N)` lookup, utilising
/// Google's [jump hash] internally. Adding 1 additional shard causes
/// approximately `1/N` keys to be remapped.
///
/// [jump hash]: https://arxiv.org/ftp/arxiv/papers/1406/1406.2294.pdf
#[derive(Debug)]
pub struct TableNamespaceSharder<T> {
    hasher: SipHasher13,
    shards: Vec<T>,
}

impl<T> TableNamespaceSharder<T> {
    /// Initialise a [`TableNamespaceSharder`] that consistently maps keys to
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
        assert!(
            !shards.is_empty(),
            "cannot initialise sharder with no shards"
        );

        Self {
            hasher: SipHasher13::new_with_key(&key),
            shards,
        }
    }
}

impl<T> TableNamespaceSharder<T> {
    /// Reinitialise [`Self`] with a new key.
    ///
    /// Re-keying [`Self`] will change the mapping of inputs to output instances
    /// of `T`.
    pub fn with_seed_key(self, key: &[u8; 16]) -> Self {
        let hasher = SipHasher13::new_with_key(key);
        Self { hasher, ..self }
    }

    /// Consistently hash `key` to a `T`.
    fn hash<H>(&self, key: H) -> &T
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
}

impl<T> FromIterator<T> for TableNamespaceSharder<T> {
    fn from_iter<U: IntoIterator<Item = T>>(iter: U) -> Self {
        Self::new(iter)
    }
}

#[derive(Hash)]
struct HashKey<'a> {
    table: &'a str,
    namespace: &'a str,
}

/// A [`TableNamespaceSharder`] is generic over `P`, the payload type, enabling
/// it to map any type of payload to a shard as it only considers the table name
/// and namespace when making a sharding decision.
impl<T, P> Sharder<P> for TableNamespaceSharder<T>
where
    T: Debug + Send + Sync,
{
    type Item = T;

    fn shard(&self, table: &str, namespace: &DatabaseName<'_>, _payload: &P) -> &Self::Item {
        // The derived hash impl for HashKey is hardened against prefix
        // collisions when combining the two fields.
        self.hash(&HashKey {
            table,
            namespace: namespace.as_ref(),
        })
    }
}

#[cfg(test)]
mod tests {
    use hashbrown::HashMap;

    use super::*;

    #[test]
    fn test_consistent_hashing() {
        const NUM_TESTS: usize = 10_000;
        const NUM_SHARDS: usize = 10;

        let hasher = TableNamespaceSharder::new(0..NUM_SHARDS);

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
        let hasher = TableNamespaceSharder::new(0..NUM_SHARDS);

        // And assert the mappings are the same
        assert!(mappings
            .iter()
            .all(|(&key, &value)| hasher.hash(key) == value));

        // Reinitialise the hasher with the a different key
        let hasher = TableNamespaceSharder::new(0..NUM_SHARDS).with_seed_key(&[42; 16]);

        // And assert the mappings are the NOT all same (some may be the same)
        assert!(!mappings
            .iter()
            .all(|(&key, &value)| hasher.hash(key) == value));
    }

    #[test]
    fn test_sharder_impl() {
        let hasher = TableNamespaceSharder::new(0..10_000);

        let a = hasher.shard("table", &DatabaseName::try_from("namespace").unwrap(), &0);
        let b = hasher.shard("table", &DatabaseName::try_from("namespace2").unwrap(), &0);
        assert_ne!(a, b);

        let a = hasher.shard("table", &DatabaseName::try_from("namespace").unwrap(), &0);
        let b = hasher.shard("table2", &DatabaseName::try_from("namespace").unwrap(), &0);
        assert_ne!(a, b);

        // Assert payloads are ignored for this sharder
        let a = hasher.shard("table", &DatabaseName::try_from("namespace").unwrap(), &0);
        let b = hasher.shard("table", &DatabaseName::try_from("namespace").unwrap(), &42);
        assert_eq!(a, b);
    }

    #[test]
    fn test_sharder_prefix_collision() {
        let hasher = TableNamespaceSharder::new(0..10_000);
        let a = hasher.shard("a", &DatabaseName::try_from("bc").unwrap(), &0);
        let b = hasher.shard("ab", &DatabaseName::try_from("c").unwrap(), &0);
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
        let hasher = TableNamespaceSharder::new(0..1000);
        let namespace = DatabaseName::try_from("bananas").unwrap();

        assert_eq!(*hasher.shard("42", &namespace, &0), 904);
        assert_eq!(*hasher.shard("4242", &namespace, &1), 230);
        assert_eq!(*hasher.shard("bananas", &namespace, &2), 183);
    }
}
