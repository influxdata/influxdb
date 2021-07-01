use std::convert::{TryFrom, TryInto};
use std::num::{NonZeroU32, NonZeroU64, NonZeroUsize};

use data_types::database_rules::{
    LifecycleRules, Sort, SortOrder, DEFAULT_CATALOG_TRANSACTIONS_UNTIL_CHECKPOINT,
    DEFAULT_LATE_ARRIVE_WINDOW_SECONDS, DEFAULT_PERSIST_AGE_THRESHOLD_SECONDS,
    DEFAULT_PERSIST_ROW_THRESHOLD, DEFAULT_WORKER_BACKOFF_MILLIS,
};

use crate::google::protobuf::Empty;
use crate::google::{FieldViolation, FromField, FromFieldOpt, FromFieldString};
use crate::influxdata::iox::management::v1 as management;

impl From<LifecycleRules> for management::LifecycleRules {
    fn from(config: LifecycleRules) -> Self {
        Self {
            mutable_linger_seconds: config
                .mutable_linger_seconds
                .map(Into::into)
                .unwrap_or_default(),
            mutable_minimum_age_seconds: config
                .mutable_minimum_age_seconds
                .map(Into::into)
                .unwrap_or_default(),
            mutable_size_threshold: config
                .mutable_size_threshold
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            buffer_size_soft: config
                .buffer_size_soft
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            buffer_size_hard: config
                .buffer_size_hard
                .map(|x| x.get() as u64)
                .unwrap_or_default(),
            sort_order: Some(config.sort_order.into()),
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
            mutable_linger_seconds: proto.mutable_linger_seconds.try_into().ok(),
            mutable_minimum_age_seconds: proto.mutable_minimum_age_seconds.try_into().ok(),
            mutable_size_threshold: (proto.mutable_size_threshold as usize).try_into().ok(),
            buffer_size_soft: (proto.buffer_size_soft as usize).try_into().ok(),
            buffer_size_hard: (proto.buffer_size_hard as usize).try_into().ok(),
            sort_order: proto.sort_order.optional("sort_order")?.unwrap_or_default(),
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

impl From<SortOrder> for management::lifecycle_rules::SortOrder {
    fn from(ps: SortOrder) -> Self {
        let order: management::Order = ps.order.into();

        Self {
            order: order as _,
            sort: Some(ps.sort.into()),
        }
    }
}

impl TryFrom<management::lifecycle_rules::SortOrder> for SortOrder {
    type Error = FieldViolation;

    fn try_from(proto: management::lifecycle_rules::SortOrder) -> Result<Self, Self::Error> {
        Ok(Self {
            order: proto.order().scope("order")?,
            sort: proto.sort.optional("sort")?.unwrap_or_default(),
        })
    }
}

impl From<Sort> for management::lifecycle_rules::sort_order::Sort {
    fn from(ps: Sort) -> Self {
        use management::lifecycle_rules::sort_order::ColumnSort;

        match ps {
            Sort::LastWriteTime => Self::LastWriteTime(Empty {}),
            Sort::CreatedAtTime => Self::CreatedAtTime(Empty {}),
            Sort::Column(column_name, column_type, column_value) => {
                let column_type: management::ColumnType = column_type.into();
                let column_value: management::Aggregate = column_value.into();

                Self::Column(ColumnSort {
                    column_name,
                    column_type: column_type as _,
                    column_value: column_value as _,
                })
            }
        }
    }
}

impl TryFrom<management::lifecycle_rules::sort_order::Sort> for Sort {
    type Error = FieldViolation;

    fn try_from(proto: management::lifecycle_rules::sort_order::Sort) -> Result<Self, Self::Error> {
        use management::lifecycle_rules::sort_order::Sort;

        Ok(match proto {
            Sort::LastWriteTime(_) => Self::LastWriteTime,
            Sort::CreatedAtTime(_) => Self::CreatedAtTime,
            Sort::Column(column_sort) => {
                let column_type = column_sort.column_type().scope("column.column_type")?;
                let column_value = column_sort.column_value().scope("column.column_value")?;
                Self::Column(
                    column_sort.column_name.required("column.column_name")?,
                    column_type,
                    column_value,
                )
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use data_types::database_rules::{ColumnType, ColumnValue, Order};

    use super::*;

    #[test]
    fn lifecycle_rules() {
        let protobuf = management::LifecycleRules {
            mutable_linger_seconds: 123,
            mutable_minimum_age_seconds: 5345,
            mutable_size_threshold: 232,
            buffer_size_soft: 353,
            buffer_size_hard: 232,
            sort_order: None,
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

        assert_eq!(config.sort_order, SortOrder::default());
        assert_eq!(
            config.mutable_linger_seconds.unwrap().get(),
            protobuf.mutable_linger_seconds
        );
        assert_eq!(
            config.mutable_minimum_age_seconds.unwrap().get(),
            protobuf.mutable_minimum_age_seconds
        );
        assert_eq!(
            config.mutable_size_threshold.unwrap().get(),
            protobuf.mutable_size_threshold as usize
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
        assert_eq!(
            back.mutable_minimum_age_seconds,
            protobuf.mutable_minimum_age_seconds
        );
        assert_eq!(back.mutable_size_threshold, protobuf.mutable_size_threshold);
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

    #[test]
    fn sort_order_default() {
        let protobuf: management::lifecycle_rules::SortOrder = Default::default();
        let config: SortOrder = protobuf.try_into().unwrap();

        assert_eq!(config, SortOrder::default());
        assert_eq!(config.order, Order::default());
        assert_eq!(config.sort, Sort::default());
    }

    #[test]
    fn sort_order() {
        use management::lifecycle_rules::sort_order;
        let protobuf = management::lifecycle_rules::SortOrder {
            order: management::Order::Asc as _,
            sort: Some(sort_order::Sort::CreatedAtTime(Empty {})),
        };
        let config: SortOrder = protobuf.clone().try_into().unwrap();
        let back: management::lifecycle_rules::SortOrder = config.clone().into();

        assert_eq!(protobuf, back);
        assert_eq!(config.order, Order::Asc);
        assert_eq!(config.sort, Sort::CreatedAtTime);
    }

    #[test]
    fn sort() {
        use management::lifecycle_rules::sort_order;

        let created_at: Sort = sort_order::Sort::CreatedAtTime(Empty {})
            .try_into()
            .unwrap();
        let last_write: Sort = sort_order::Sort::LastWriteTime(Empty {})
            .try_into()
            .unwrap();
        let column: Sort = sort_order::Sort::Column(sort_order::ColumnSort {
            column_name: "column".to_string(),
            column_type: management::ColumnType::Bool as _,
            column_value: management::Aggregate::Min as _,
        })
        .try_into()
        .unwrap();

        assert_eq!(created_at, Sort::CreatedAtTime);
        assert_eq!(last_write, Sort::LastWriteTime);
        assert_eq!(
            column,
            Sort::Column("column".to_string(), ColumnType::Bool, ColumnValue::Min)
        );
    }

    #[test]
    fn partition_sort_column_sort() {
        use management::lifecycle_rules::sort_order;

        let res: Result<Sort, _> = sort_order::Sort::Column(Default::default()).try_into();
        let err1 = res.expect_err("expected failure");

        let res: Result<Sort, _> = sort_order::Sort::Column(sort_order::ColumnSort {
            column_type: management::ColumnType::F64 as _,
            ..Default::default()
        })
        .try_into();
        let err2 = res.expect_err("expected failure");

        let res: Result<Sort, _> = sort_order::Sort::Column(sort_order::ColumnSort {
            column_type: management::ColumnType::F64 as _,
            column_value: management::Aggregate::Max as _,
            ..Default::default()
        })
        .try_into();
        let err3 = res.expect_err("expected failure");

        assert_eq!(err1.field, "column.column_type");
        assert_eq!(err1.description, "Field is required");

        assert_eq!(err2.field, "column.column_value");
        assert_eq!(err2.description, "Field is required");

        assert_eq!(err3.field, "column.column_name");
        assert_eq!(err3.description, "Field is required");
    }
}
