use crate::influxdata::iox::ingester::v1 as proto;
use data_types::ShardWriteStatus;
use std::collections::HashMap;

impl From<ShardWriteStatus> for proto::ShardStatus {
    fn from(status: ShardWriteStatus) -> Self {
        match status {
            ShardWriteStatus::ShardUnknown => Self::Unknown,
            ShardWriteStatus::Durable => Self::Durable,
            ShardWriteStatus::Readable => Self::Readable,
            ShardWriteStatus::Persisted => Self::Persisted,
        }
    }
}

impl proto::ShardStatus {
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

impl proto::ShardInfo {
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
    // Map shard index to status
    let mut shard_infos: HashMap<_, proto::ShardInfo> = HashMap::new();

    responses
        .into_iter()
        .flat_map(|res| res.shard_infos.into_iter())
        .for_each(|info| {
            shard_infos
                .entry(info.shard_index)
                .and_modify(|existing_info| existing_info.merge(&info))
                .or_insert(info);
        });

    let shard_infos = shard_infos.into_values().collect();

    proto::GetWriteInfoResponse { shard_infos }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proto::{ShardInfo, ShardStatus};

    #[test]
    fn test_merge() {
        #[derive(Debug)]
        struct Test<'a> {
            left: &'a ShardInfo,
            right: &'a ShardInfo,
            expected: &'a ShardInfo,
        }

        let durable = ShardInfo {
            shard_index: 1,
            status: ShardStatus::Durable.into(),
        };

        let readable = ShardInfo {
            shard_index: 1,
            status: ShardStatus::Readable.into(),
        };

        let persisted = ShardInfo {
            shard_index: 1,
            status: ShardStatus::Persisted.into(),
        };

        let unknown = ShardInfo {
            shard_index: 1,
            status: ShardStatus::Unknown.into(),
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
                "Mismatch\n\nOutput:\n{output:#?}\n\nTest:\n{test:#?}"
            );
        }
    }
}
