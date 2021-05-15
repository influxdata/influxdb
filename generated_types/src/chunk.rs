use crate::google::{FieldViolation, FromField};
use crate::influxdata::iox::management::v1 as management;
use data_types::chunk_metadata::{ChunkStorage, ChunkSummary};
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
            estimated_bytes,
            row_count,
            time_of_first_write,
            time_of_last_write,
            time_closed,
        } = summary;

        let storage: management::ChunkStorage = storage.into();
        let storage = storage.into(); // convert to i32

        let estimated_bytes = estimated_bytes as u64;
        let row_count = row_count as u64;

        let partition_key = partition_key.to_string();
        let table_name = table_name.to_string();

        let time_of_first_write = time_of_first_write.map(|t| t.into());
        let time_of_last_write = time_of_last_write.map(|t| t.into());
        let time_closed = time_closed.map(|t| t.into());

        Self {
            partition_key,
            table_name,
            id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write,
            time_of_last_write,
            time_closed,
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

/// Conversion code from management API chunk structure
impl TryFrom<management::Chunk> for ChunkSummary {
    type Error = FieldViolation;

    fn try_from(proto: management::Chunk) -> Result<Self, Self::Error> {
        // Use prost enum conversion
        let storage = proto.storage().scope("storage")?;

        let time_of_first_write = proto
            .time_of_first_write
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "time_of_first_write".to_string(),
                description: "Timestamp must be positive".to_string(),
            })?;

        let time_of_last_write = proto
            .time_of_last_write
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "time_of_last_write".to_string(),
                description: "Timestamp must be positive".to_string(),
            })?;

        let time_closed = proto
            .time_closed
            .map(TryInto::try_into)
            .transpose()
            .map_err(|_| FieldViolation {
                field: "time_closed".to_string(),
                description: "Timestamp must be positive".to_string(),
            })?;

        let management::Chunk {
            partition_key,
            table_name,
            id,
            estimated_bytes,
            row_count,
            ..
        } = proto;

        let estimated_bytes = estimated_bytes as usize;
        let row_count = row_count as usize;
        let partition_key = Arc::from(partition_key.as_str());
        let table_name = Arc::from(table_name.as_str());

        Ok(Self {
            partition_key,
            table_name,
            id,
            storage,
            estimated_bytes,
            row_count,
            time_of_first_write,
            time_of_last_write,
            time_closed,
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn valid_proto_to_summary() {
        let proto = management::Chunk {
            partition_key: "foo".to_string(),
            table_name: "bar".to_string(),
            id: 42,
            estimated_bytes: 1234,
            row_count: 321,
            storage: management::ChunkStorage::ObjectStoreOnly.into(),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        };

        let summary = ChunkSummary::try_from(proto).expect("conversion successful");
        let expected = ChunkSummary {
            partition_key: Arc::from("foo"),
            table_name: Arc::from("bar"),
            id: 42,
            estimated_bytes: 1234,
            row_count: 321,
            storage: ChunkStorage::ObjectStoreOnly,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
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
            estimated_bytes: 1234,
            row_count: 321,
            storage: ChunkStorage::ObjectStoreOnly,
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        };

        let proto = management::Chunk::try_from(summary).expect("conversion successful");

        let expected = management::Chunk {
            partition_key: "foo".to_string(),
            table_name: "bar".to_string(),
            id: 42,
            estimated_bytes: 1234,
            row_count: 321,
            storage: management::ChunkStorage::ObjectStoreOnly.into(),
            time_of_first_write: None,
            time_of_last_write: None,
            time_closed: None,
        };

        assert_eq!(
            proto, expected,
            "Actual:\n\n{:?}\n\nExpected:\n\n{:?}\n\n",
            proto, expected
        );
    }
}
