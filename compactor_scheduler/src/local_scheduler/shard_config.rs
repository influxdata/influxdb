use clap_blocks::compactor_scheduler::ShardConfigForLocalScheduler;

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

impl ShardConfig {
    /// Create a new [`ShardConfig`] from a [`ShardConfigForLocalScheduler`].
    pub fn from_config(config: ShardConfigForLocalScheduler) -> Option<Self> {
        match (config.shard_count, config.shard_id, config.hostname) {
            // if no shard_count is provided, then we are not sharding
            (None, _, _) => None,
            // always use the shard_id if provided
            (Some(shard_count), Some(shard_id), _) => Some(ShardConfig {
                shard_id,
                n_shards: shard_count,
            }),
            // if no shard_id is provided, then we are sharding by hostname
            (Some(shard_count), None, Some(hostname)) => {
                let parsed_id = hostname
                    .chars()
                    .skip_while(|ch| !ch.is_ascii_digit())
                    .take_while(|ch| ch.is_ascii_digit())
                    .fold(None, |acc, ch| {
                        ch.to_digit(10).map(|b| acc.unwrap_or(0) * 10 + b)
                    });
                assert!(parsed_id.is_some(), "hostname must end in a shard ID");
                Some(ShardConfig {
                    shard_id: parsed_id.unwrap() as usize,
                    n_shards: shard_count,
                })
            }
            (Some(_), None, None) => {
                panic!("shard_count must be paired with either shard_id or hostname")
            }
        }
    }
}
