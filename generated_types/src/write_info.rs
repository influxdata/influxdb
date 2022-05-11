use crate::influxdata::iox::ingester::v1 as proto;
use data_types::KafkaPartitionWriteStatus;
use std::collections::HashMap;

impl From<KafkaPartitionWriteStatus> for proto::KafkaPartitionStatus {
    fn from(status: KafkaPartitionWriteStatus) -> Self {
        match status {
            KafkaPartitionWriteStatus::KafkaPartitionUnknown => Self::Unknown,
            KafkaPartitionWriteStatus::Durable => Self::Durable,
            KafkaPartitionWriteStatus::Readable => Self::Readable,
            KafkaPartitionWriteStatus::Persisted => Self::Persisted,
        }
    }
}

impl proto::KafkaPartitionStatus {
    /// Convert the status to a number such that higher numbers are later in the data lifecycle.
    /// For use in merging multiple write status gRPC responses into one response.
    fn status_order(&self) -> u8 {
        match self {
            Self::Unspecified => panic!("Unspecified status"),
            Self::Unknown => 0,
            Self::Durable => 1,
            Self::Readable => 2,
            Self::Persisted => 3,
        }
    }
}

impl proto::KafkaPartitionInfo {
    fn merge(&mut self, other: &Self) {
        let self_status = self.status();
        let other_status = other.status();

        let new_status = match self_status.status_order().cmp(&other_status.status_order()) {
            std::cmp::Ordering::Less => other_status,
            std::cmp::Ordering::Equal => self_status,
            std::cmp::Ordering::Greater => self_status,
        };

        self.set_status(new_status);
    }
}

/// "Merges" the partition information for write info responses so that the "most recent"
/// information is returned.
pub fn merge_responses(
    responses: impl IntoIterator<Item = proto::GetWriteInfoResponse>,
) -> proto::GetWriteInfoResponse {
    // Map kafka partition id to status
    let mut partition_infos: HashMap<_, proto::KafkaPartitionInfo> = HashMap::new();

    responses
        .into_iter()
        .flat_map(|res| res.kafka_partition_infos.into_iter())
        .for_each(|info| {
            partition_infos
                .entry(info.kafka_partition_id)
                .and_modify(|existing_info| existing_info.merge(&info))
                .or_insert(info);
        });

    let kafka_partition_infos = partition_infos
        .into_iter()
        .map(|(_kafka_partition_id, info)| info)
        .collect();

    proto::GetWriteInfoResponse {
        kafka_partition_infos,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{KafkaPartitionInfo, KafkaPartitionStatus};

    #[test]
    fn test_merge() {
        #[derive(Debug)]
        struct Test<'a> {
            left: &'a KafkaPartitionInfo,
            right: &'a KafkaPartitionInfo,
            expected: &'a KafkaPartitionInfo,
        }

        let durable = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Durable.into(),
        };

        let readable = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Readable.into(),
        };

        let persisted = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Persisted.into(),
        };

        let unknown = KafkaPartitionInfo {
            kafka_partition_id: 1,
            status: KafkaPartitionStatus::Unknown.into(),
        };

        let tests = vec![
            Test {
                left: &unknown,
                right: &unknown,
                expected: &unknown,
            },
            Test {
                left: &unknown,
                right: &durable,
                expected: &durable,
            },
            Test {
                left: &unknown,
                right: &readable,
                expected: &readable,
            },
            Test {
                left: &durable,
                right: &unknown,
                expected: &durable,
            },
            Test {
                left: &readable,
                right: &readable,
                expected: &readable,
            },
            Test {
                left: &durable,
                right: &durable,
                expected: &durable,
            },
            Test {
                left: &readable,
                right: &durable,
                expected: &readable,
            },
            Test {
                left: &persisted,
                right: &durable,
                expected: &persisted,
            },
        ];

        for test in tests {
            let mut output = test.left.clone();

            output.merge(test.right);
            assert_eq!(
                &output, test.expected,
                "Mismatch\n\nOutput:\n{:#?}\n\nTest:\n{:#?}",
                output, test
            );
        }
    }
}
