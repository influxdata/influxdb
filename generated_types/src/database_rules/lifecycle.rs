use std::convert::{TryFrom, TryInto};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use data_types::database_rules::{
    LifecycleRules, DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT,
    DEFAULT_LATE_ARRIVE_WINDOW_SECONDS, DEFAULT_MUB_ROW_THRESHOLD,
    DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS, DEFAULT_PERSIST_ROW_THRESHOLD,
    DEFAULT_WORKER_BACKOFF_MILLIS,
};

use crate::google::FieldViolation;
use crate::influxdata::iox::management::v1 as management;

impl From<LifecycleRules> for management::LifecycleRules {
    fn from(config: LifecycleRules) -> Self {
        #[allow(deprecated)]
        Self {
            buffer_size_soft: config
                .buffer_size_soft
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            buffer_size_hard: config
                .buffer_size_hard
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            drop_non_persisted: config.drop_non_persisted,
            persist: config.persist,
            immutable: config.immutable,
            worker_backoff_millis: config.worker_backoff_millis.get(),
            max_active_compactions: config.max_active_compactions.get(),
            catalog_transactions_until_checkpoint: config
                .catalog_transactions_until_checkpoint
                .get(),
            late_arrive_window_seconds: config.late_arrive_window_seconds.get(),
            persist_row_threshold: config.persist_row_threshold.get() as u64,
            persist_age_threshold_seconds: config.persist_age_threshold_seconds.get(),
            mub_row_threshold: config.mub_row_threshold.get() as u64,
            parquet_cache_limit: config
                .parquet_cache_limit
                .map(|v| v.get())
                .unwrap_or_default(),
        }
    }
}

impl TryFrom<management::LifecycleRules> for LifecycleRules {
    type Error = FieldViolation;

    fn try_from(proto: management::LifecycleRules) -> Result<Self, Self::Error> {
        Ok(Self {
            buffer_size_soft: (proto.buffer_size_soft as usize).try_into().ok(),
            buffer_size_hard: (proto.buffer_size_hard as usize).try_into().ok(),
            drop_non_persisted: proto.drop_non_persisted,
            persist: proto.persist,
            immutable: proto.immutable,
            worker_backoff_millis: NonZeroU64::new(proto.worker_backoff_millis)
                .unwrap_or_else(|| NonZeroU64::new(DEFAULT_WORKER_BACKOFF_MILLIS).unwrap()),
            max_active_compactions: NonZeroU32::new(proto.max_active_compactions)
                .unwrap_or_else(|| NonZeroU32::new(num_cpus::get() as u32).unwrap()), // default to num CPU threads
            catalog_transactions_until_checkpoint: NonZeroU64::new(
                proto.catalog_transactions_until_checkpoint,
            )
            .unwrap_or_else(|| {
                NonZeroU64::new(DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT).unwrap()
            }),
            late_arrive_window_seconds: NonZeroU32::new(proto.late_arrive_window_seconds)
                .unwrap_or_else(|| NonZeroU32::new(DEFAULT_LATE_ARRIVE_WINDOW_SECONDS).unwrap()),
            persist_row_threshold: NonZeroUsize::new(proto.persist_row_threshold as usize)
                .unwrap_or_else(|| {
                    NonZeroUsize::new(DEFAULT_PERSIST_ROW_THRESHOLD as usize).unwrap()
                }),
            persist_age_threshold_seconds: NonZeroU32::new(proto.persist_age_threshold_seconds)
                .unwrap_or_else(|| NonZeroU32::new(DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS).unwrap()),
            mub_row_threshold: NonZeroUsize::new(proto.mub_row_threshold as usize)
                .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_MUB_ROW_THRESHOLD).unwrap()),
            parquet_cache_limit: NonZeroU64::new(proto.parquet_cache_limit),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_rules() {
        #[allow(deprecated)]
        let protobuf = management::LifecycleRules {
            buffer_size_soft: 353,
            buffer_size_hard: 232,
            drop_non_persisted: true,
            persist: true,
            immutable: true,
            worker_backoff_millis: 1000,
            max_active_compactions: 8,
            catalog_transactions_until_checkpoint: 10,
            late_arrive_window_seconds: 23,
            persist_row_threshold: 57,
            persist_age_threshold_seconds: 23,
            mub_row_threshold: 3454,
            parquet_cache_limit: 10,
        };

        let config: LifecycleRules = protobuf.clone().try_into().unwrap();
        let back: management::LifecycleRules = config.clone().into();

        assert_eq!(
            config.buffer_size_soft.unwrap().get(),
            protobuf.buffer_size_soft as usize
        );
        assert_eq!(
            config.buffer_size_hard.unwrap().get(),
            protobuf.buffer_size_hard as usize
        );
        assert_eq!(config.drop_non_persisted, protobuf.drop_non_persisted);
        assert_eq!(config.immutable, protobuf.immutable);

        assert_eq!(back.buffer_size_soft, protobuf.buffer_size_soft);
        assert_eq!(back.buffer_size_hard, protobuf.buffer_size_hard);
        assert_eq!(back.drop_non_persisted, protobuf.drop_non_persisted);
        assert_eq!(back.immutable, protobuf.immutable);
        assert_eq!(back.worker_backoff_millis, protobuf.worker_backoff_millis);
        assert_eq!(back.max_active_compactions, protobuf.max_active_compactions);
        assert_eq!(
            back.late_arrive_window_seconds,
            protobuf.late_arrive_window_seconds
        );
        assert_eq!(back.persist_row_threshold, protobuf.persist_row_threshold);
        assert_eq!(
            back.persist_age_threshold_seconds,
            protobuf.persist_age_threshold_seconds
        );
        assert_eq!(back.mub_row_threshold, protobuf.mub_row_threshold);
        assert_eq!(
            config.parquet_cache_limit.unwrap().get(),
            protobuf.parquet_cache_limit
        );
        assert_eq!(back.parquet_cache_limit, protobuf.parquet_cache_limit);
    }

    #[test]
    fn lifecycle_rules_default() {
        let protobuf = management::LifecycleRules::default();
        let config: LifecycleRules = protobuf.try_into().unwrap();
        assert_eq!(config, LifecycleRules::default());
    }
}
