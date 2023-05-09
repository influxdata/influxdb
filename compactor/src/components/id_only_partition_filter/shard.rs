use std::fmt::Display;

use data_types::PartitionId;
use sharder::JumpHash;

use super::IdOnlyPartitionFilter;

#[derive(Debug)]
pub struct ShardPartitionFilter {
    jump_hash: JumpHash<usize>,
    shard_id: usize,
}

impl ShardPartitionFilter {
    pub fn new(n_shards: usize, shard_id: usize) -> Self {
        assert!(shard_id < n_shards, "shard_id out of range");

        Self {
            jump_hash: JumpHash::new(0..n_shards),
            shard_id,
        }
    }
}

impl Display for ShardPartitionFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard")
    }
}

impl IdOnlyPartitionFilter for ShardPartitionFilter {
    fn apply(&self, partition_id: PartitionId) -> bool {
        *self.jump_hash.hash(partition_id) == self.shard_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display() {
        assert_eq!(ShardPartitionFilter::new(1, 0).to_string(), "shard");
    }

    #[test]
    #[should_panic(expected = "shard_id out of range")]
    fn test_new_checks_shard_it_range() {
        ShardPartitionFilter::new(1, 1);
    }

    #[test]
    fn test_apply() {
        let n_shards = 3usize;
        let mut filters = (0..n_shards)
            .map(|shard_id| (ShardPartitionFilter::new(n_shards, shard_id), 0))
            .collect::<Vec<_>>();

        for pid in 0..100 {
            let pid = PartitionId::new(pid);

            let mut hit = false;
            for (filter, counter) in &mut filters {
                if filter.apply(pid) {
                    assert!(!hit, "only one hit allowed");
                    hit = true;
                    *counter += 1;
                }
            }

            assert!(hit, "at least one hit required");
        }

        for (_filter, counter) in filters {
            assert!(counter > 10, "good distribution");
        }
    }
}
