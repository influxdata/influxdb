use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// A ConsistentHasher implements a simple consistent hashing mechanism
/// that maps a point to the nearest "node" N.
///
/// It has the property that the addition or removal of one node in the ring
/// in the worst case only changes the mapping of points that were assigned
/// to the node adjacent to the node that gets inserted/removed (on average half
/// of them).
///
/// e.g. you can use it find the ShardID in vector of ShardIds
/// that is closest to a given hash value.
#[derive(Debug, Eq, PartialEq, Clone, Default)]
pub struct ConsistentHasher<T>
where
    T: Copy + Hash,
{
    ring: Vec<(u64, T)>,
}

impl<T> ConsistentHasher<T>
where
    T: Copy + Hash,
{
    pub fn new(nodes: &[T]) -> Self {
        let mut ring: Vec<_> = nodes.iter().map(|node| (Self::hash(node), *node)).collect();
        ring.sort_by_key(|(hash, _)| *hash);
        Self { ring }
    }

    pub fn find<H: Hash>(&self, point: H) -> Option<T> {
        let point_hash = Self::hash(point);
        self.ring
            .iter()
            .find(|(node_hash, _)| node_hash > &point_hash)
            .or_else(|| self.ring.first())
            .map(|(_, node)| *node)
    }

    pub fn is_empty(&self) -> bool {
        self.ring.is_empty()
    }

    pub fn len(&self) -> usize {
        self.ring.len()
    }

    fn hash<H: Hash>(h: H) -> u64 {
        let mut hasher = DefaultHasher::new();
        h.hash(&mut hasher);
        hasher.finish()
    }
}

impl<T> From<ConsistentHasher<T>> for Vec<T>
where
    T: Copy + Hash,
{
    fn from(hasher: ConsistentHasher<T>) -> Self {
        hasher.ring.into_iter().map(|(_, node)| node).collect()
    }
}

impl<T> From<Vec<T>> for ConsistentHasher<T>
where
    T: Copy + Hash,
{
    fn from(vec: Vec<T>) -> Self {
        Self::new(&vec)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_roundtrip() {}

    #[test]
    fn test_consistent_hasher() {
        let ch = ConsistentHasher::new(&[10, 20, 30, 40]);

        // test points found with:
        /*
               for needle in (10..=40).step_by(10) {
                   let mut found = 0;
                   for point in 0..100 {
                       if ch.find(point) == Some(needle) {
                           found += 1;
                           println!(r#"assert_eq!(ch.find({}), Some({}));"#, point, needle);
                       }
                       if found >= 16 {
                           break;
                       }
                   }
                   println!();
               }
        */

        assert_eq!(ch.find(1), Some(10));
        assert_eq!(ch.find(6), Some(10));
        assert_eq!(ch.find(16), Some(10));
        assert_eq!(ch.find(25), Some(10));

        assert_eq!(ch.find(8), Some(20));
        assert_eq!(ch.find(9), Some(20));
        assert_eq!(ch.find(11), Some(20));
        assert_eq!(ch.find(13), Some(20));

        assert_eq!(ch.find(3), Some(30));
        assert_eq!(ch.find(12), Some(30));
        assert_eq!(ch.find(15), Some(30));
        assert_eq!(ch.find(20), Some(30));

        assert_eq!(ch.find(7), Some(40));
        assert_eq!(ch.find(10), Some(40));
        assert_eq!(ch.find(14), Some(40));
        assert_eq!(ch.find(18), Some(40));

        let ch = ConsistentHasher::new(&[10, 20, 30, 40, 50]);

        assert_eq!(ch.find(1), Some(10));
        assert_eq!(ch.find(6), Some(10));
        assert_eq!(ch.find(16), Some(10));
        assert_eq!(ch.find(25), Some(10));

        assert_eq!(ch.find(8), Some(20));
        assert_eq!(ch.find(9), Some(20));
        assert_eq!(ch.find(11), Some(50)); // <-- moved to node 50
        assert_eq!(ch.find(13), Some(50)); // <-- moved to node 50

        assert_eq!(ch.find(3), Some(30));
        assert_eq!(ch.find(12), Some(30));
        assert_eq!(ch.find(15), Some(30));
        assert_eq!(ch.find(20), Some(30));

        assert_eq!(ch.find(7), Some(40));
        assert_eq!(ch.find(10), Some(40));
        assert_eq!(ch.find(14), Some(40));
        assert_eq!(ch.find(18), Some(40));

        let ch = ConsistentHasher::new(&[10, 20, 30]);

        assert_eq!(ch.find(1), Some(10));
        assert_eq!(ch.find(6), Some(10));
        assert_eq!(ch.find(16), Some(10));
        assert_eq!(ch.find(25), Some(10));

        assert_eq!(ch.find(8), Some(20));
        assert_eq!(ch.find(9), Some(20));
        assert_eq!(ch.find(11), Some(20));
        assert_eq!(ch.find(13), Some(20));

        assert_eq!(ch.find(3), Some(30));
        assert_eq!(ch.find(12), Some(30));
        assert_eq!(ch.find(15), Some(30));
        assert_eq!(ch.find(20), Some(30));

        // all points that used to map to shard 40 go to shard 20
        assert_eq!(ch.find(7), Some(20));
        assert_eq!(ch.find(10), Some(20));
        assert_eq!(ch.find(14), Some(20));
        assert_eq!(ch.find(18), Some(20));
    }
}
