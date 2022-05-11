use crate::influxdata::iox::ingester::v1 as proto;
use data_types::KafkaPartitionWriteStatus;

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
