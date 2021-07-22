use crate::google::{FieldViolation, FromFieldOpt};
use crate::influxdata::iox::management::v1 as management;
use data_types::chunk_metadata::{ChunkLifecycleAction, ChunkStorage, ChunkSummary};
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

/// Conversion code to management API chunk structure
impl From<ChunkSummary> for management::Chunk {
    fn from(summary: ChunkSummary) -> Self {
        let ChunkSummary {
            partition_key,
            table_name,
            id,
            storage,
            lifecycle_action,
            memory_bytes,
            object_store_bytes,
            row_count,
            time_of_last_access,
            time_of_first_write,
            time_of_last_write,
            time_closed,
        } = summary;

        Self {
            partition_key: partition_key.to_string(),
            table_name: table_name.to_string(),
            id,
            storage: management::ChunkStorage::from(storage).into(),
            lifecycle_action: management::ChunkLifecycleAction::from(lifecycle_action).into(),
            memory_bytes: memory_bytes as u64,
            object_store_bytes: object_store_bytes as u64,
            row_count: row_count as u64,
            time_of_last_access: time_of_last_access.map(Into::into),
            time_of_first_write: time_of_first_write.map(Into::into),
            time_of_last_write: time_of_last_write.map(Into::into),
            time_closed: time_closed.map(Into::into),
        }
    }
}

impl From<ChunkStorage> for management::ChunkStorage {
    fn from(storage: ChunkStorage) -> Self {
        match storage {
            ChunkStorage::OpenMutableBuffer => Self::OpenMutableBuffer,
            ChunkStorage::ClosedMutableBuffer => Self::ClosedMutableBuffer,
            ChunkStorage::ReadBuffer => Self::ReadBuffer,
            ChunkStorage::ReadBufferAndObjectStore => Self::ReadBufferAndObjectStore,
            ChunkStorage::ObjectStoreOnly => Self::ObjectStoreOnly,
        }
    }
}

impl From<Option<ChunkLifecycleAction>> for management::ChunkLifecycleAction {
    fn from(lifecycle_action: Option<ChunkLifecycleAction>) -> Self {
        match lifecycle_action {
            Some(ChunkLifecycleAction::Moving) => Self::Moving,
            Some(ChunkLifecycleAction::Persisting) => Self::Persisting,
            Some(ChunkLifecycleAction::Compacting) => Self::Compacting,
            Some(ChunkLifecycleAction::Dropping) => Self::Dropping,
            None => Self::Unspecified,
        }
    }
}

/// Conversion code from management API chunk structure
impl TryFrom<management::Chunk> for ChunkSummary {
    type Error = FieldViolation;

    fn try_from(proto: management::Chunk) -> Result<Self, Self::Error> {
        let convert_timestamp = |t: google_types::protobuf::Timestamp, field: &'static str| {
            t.try_into().map_err(|_| FieldViolation {
                field: field.to_string(),
                description: "Timestamp must be positive".to_string(),
            })
        };

        let timestamp = |t: Option<google_types::protobuf::Timestamp>, field: &'static str| {
            t.map(|t| convert_timestamp(t, field)).transpose()
        };

        let management::Chunk {
            partition_key,
            table_name,
            id,
            storage,
            lifecycle_action,
            memory_bytes,
            object_store_bytes,
            row_count,
            time_of_last_access,
            time_of_first_write,
            time_of_last_write,
            time_closed,
        } = proto;

        Ok(Self {
            partition_key: Arc::from(partition_key.as_str()),
            table_name: Arc::from(table_name.as_str()),
            id,
            storage: management::ChunkStorage::from_i32(storage).required("storage")?,
            lifecycle_action: management::ChunkLifecycleAction::from_i32(lifecycle_action)
                .required("lifecycle_action")?,
            memory_bytes: memory_bytes as usize,
            object_store_bytes: object_store_bytes as usize,
            row_count: row_count as usize,
            time_of_last_access: timestamp(time_of_last_access, "time_of_last_access")?,
            time_of_first_write: timestamp(time_of_first_write, "time_of_first_write")?,
            time_of_last_write: timestamp(time_of_last_write, "time_of_last_write")?,
            time_closed: timestamp(time_closed, "time_closed")?,
        })
    }
}

impl TryFrom<management::ChunkStorage> for ChunkStorage {
    type Error = FieldViolation;

    fn try_from(proto: management::ChunkStorage) -> Result<Self, Self::Error> {
        match proto {
            management::ChunkStorage::OpenMutableBuffer => Ok(Self::OpenMutableBuffer),
            management::ChunkStorage::ClosedMutableBuffer => Ok(Self::ClosedMutableBuffer),
            management::ChunkStorage::ReadBuffer => Ok(Self::ReadBuffer),
            management::ChunkStorage::ReadBufferAndObjectStore => {
                Ok(Self::ReadBufferAndObjectStore)
            }
            management::ChunkStorage::ObjectStoreOnly => Ok(Self::ObjectStoreOnly),
            management::ChunkStorage::Unspecified => Err(FieldViolation::required("")),
        }
    }
}

impl TryFrom<management::ChunkLifecycleAction> for Option<ChunkLifecycleAction> {
    type Error = FieldViolation;

    fn try_from(proto: management::ChunkLifecycleAction) -> Result<Self, Self::Error> {
        match proto {
            management::ChunkLifecycleAction::Moving => Ok(Some(ChunkLifecycleAction::Moving)),
            management::ChunkLifecycleAction::Persisting => {
                Ok(Some(ChunkLifecycleAction::Persisting))
            }
            management::ChunkLifecycleAction::Compacting => {
                Ok(Some(ChunkLifecycleAction::Compacting))
            }
            management::ChunkLifecycleAction::Dropping => Ok(Some(ChunkLifecycleAction::Dropping)),
            management::ChunkLifecycleAction::Unspecified => Ok(None),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn valid_proto_to_summary() {
        let proto = management::Chunk {
            partition_key: "foo".to_string(),
            table_name: "bar".to_string(),
            id: 42,
            memory_bytes: 1234,
            object_store_bytes: 567,
            row_count: 321,

            storage: management::ChunkStorage::ObjectStoreOnly.into(),
            lifecycle_action: management::ChunkLifecycleAction::Moving.into(),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
            time_of_last_access: Some(google_types::protobuf::Timestamp {
                seconds: 50,
                nanos: 7,
            }),
        };

        let summary = ChunkSummary::try_from(proto).expect("conversion successful");
        let expected = ChunkSummary {
            partition_key: Arc::from("foo"),
            table_name: Arc::from("bar"),
            id: 42,
            memory_bytes: 1234,
            object_store_bytes: 567,
            row_count: 321,
            storage: ChunkStorage::ObjectStoreOnly,
            lifecycle_action: Some(ChunkLifecycleAction::Moving),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
            time_of_last_access: Some(Utc.timestamp_nanos(50_000_000_007)),
        };

        assert_eq!(
            summary, expected,
            "Actual:\n\n{:?}\n\nExpected:\n\n{:?}\n\n",
            summary, expected
        );
    }

    #[test]
    fn valid_summary_to_proto() {
        let summary = ChunkSummary {
            partition_key: Arc::from("foo"),
            table_name: Arc::from("bar"),
            id: 42,
            memory_bytes: 1234,
            object_store_bytes: 567,
            row_count: 321,
            storage: ChunkStorage::ObjectStoreOnly,
            lifecycle_action: Some(ChunkLifecycleAction::Persisting),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
            time_of_last_access: Some(Utc.timestamp_nanos(12_000_100_007)),
        };

        let proto = management::Chunk::try_from(summary).expect("conversion successful");

        let expected = management::Chunk {
            partition_key: "foo".to_string(),
            table_name: "bar".to_string(),
            id: 42,
            memory_bytes: 1234,
            object_store_bytes: 567,
            row_count: 321,
            storage: management::ChunkStorage::ObjectStoreOnly.into(),
            lifecycle_action: management::ChunkLifecycleAction::Persisting.into(),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
            time_of_last_access: Some(google_types::protobuf::Timestamp {
                seconds: 12,
                nanos: 100_007,
            }),
        };

        assert_eq!(
            proto, expected,
            "Actual:\n\n{:?}\n\nExpected:\n\n{:?}\n\n",
            proto, expected
        );
    }
}
