/// Shard config.
/// configured per LocalScheduler, which equates to per compactor.
#[derive(Debug, Clone)]
#[allow(missing_copy_implementations)]
pub struct ShardConfig {
    /// Number of shards.
    pub n_shards: usize,

    /// Shard ID.
    ///
    /// Starts as 0 and must be smaller than the number of shards.
    pub shard_id: usize,
}

impl std::fmt::Display for ShardConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self { n_shards, shard_id } = self;
        write!(
            f,
            "shard_cfg(n_shards={:?},shard_id={:?})",
            n_shards, shard_id
        )
    }
}
