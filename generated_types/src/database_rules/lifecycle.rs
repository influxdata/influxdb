use std::convert::{TryFrom, TryInto};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use data_types::database_rules::{
    LifecycleRules, DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT,
    DEFAULT_LATE_ARRIVE_WINDOW_SECONDS, DEFAULT_MUTABLE_LINGER_SECONDS,
    DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS, DEFAULT_PERSIST_ROW_THRESHOLD,
    DEFAULT_WORKER_BACKOFF_MILLIS,
};

use crate::google::FieldViolation;
use crate::influxdata::iox::management::v1 as management;

impl From<LifecycleRules> for management::LifecycleRules {
    fn from(config: LifecycleRules) -> Self {
        Self {
            mutable_linger_seconds: config.mutable_linger_seconds.get(),
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
            catalog_transactions_until_checkpoint: config
                .catalog_transactions_until_checkpoint
                .get(),
            late_arrive_window_seconds: config.late_arrive_window_seconds.get(),
            persist_row_threshold: config.persist_row_threshold.get() as u64,
            persist_age_threshold_seconds: config.persist_age_threshold_seconds.get(),
        }
    }
}

impl TryFrom<management::LifecycleRules> for LifecycleRules {
    type Error = FieldViolation;

    fn try_from(proto: management::LifecycleRules) -> Result<Self, Self::Error> {
        Ok(Self {
            mutable_linger_seconds: NonZeroU32::new(if proto.mutable_linger_seconds == 0 {
                DEFAULT_MUTABLE_LINGER_SECONDS
            } else {
                proto.mutable_linger_seconds
            })
            .unwrap(),
            buffer_size_soft: (proto.buffer_size_soft as usize).try_into().ok(),
            buffer_size_hard: (proto.buffer_size_hard as usize).try_into().ok(),
            drop_non_persisted: proto.drop_non_persisted,
            persist: proto.persist,
            immutable: proto.immutable,
            worker_backoff_millis: NonZeroU64::new(proto.worker_backoff_millis)
                .unwrap_or_else(|| NonZeroU64::new(DEFAULT_WORKER_BACKOFF_MILLIS).unwrap()),
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
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_rules() {
        let protobuf = management::LifecycleRules {
            mutable_linger_seconds: 123,
            buffer_size_soft: 353,
            buffer_size_hard: 232,
            drop_non_persisted: true,
            persist: true,
            immutable: true,
            worker_backoff_millis: 1000,
            catalog_transactions_until_checkpoint: 10,
            late_arrive_window_seconds: 23,
            persist_row_threshold: 57,
            persist_age_threshold_seconds: 23,
        };

        let config: LifecycleRules = protobuf.clone().try_into().unwrap();
        let back: management::LifecycleRules = config.clone().into();

        assert_eq!(
            config.mutable_linger_seconds.get(),
            protobuf.mutable_linger_seconds
        );
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

        assert_eq!(back.mutable_linger_seconds, protobuf.mutable_linger_seconds);
        assert_eq!(back.buffer_size_soft, protobuf.buffer_size_soft);
        assert_eq!(back.buffer_size_hard, protobuf.buffer_size_hard);
        assert_eq!(back.drop_non_persisted, protobuf.drop_non_persisted);
        assert_eq!(back.immutable, protobuf.immutable);
        assert_eq!(back.worker_backoff_millis, protobuf.worker_backoff_millis);
        assert_eq!(
            back.late_arrive_window_seconds,
            protobuf.late_arrive_window_seconds
        );
        assert_eq!(back.persist_row_threshold, protobuf.persist_row_threshold);
        assert_eq!(
            back.persist_age_threshold_seconds,
            protobuf.persist_age_threshold_seconds
        );
    }

    #[test]
    fn lifecycle_rules_default() {
        let protobuf = management::LifecycleRules::default();
        let config: LifecycleRules = protobuf.try_into().unwrap();
        assert_eq!(config, LifecycleRules::default());
    }
}
