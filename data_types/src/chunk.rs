//! Module contains a representation of chunk metadata
use std::{convert::TryFrom, sync::Arc};

use crate::field_validation::FromField;
use generated_types::{google::FieldViolation, influxdata::iox::management::v1 as management};
use serde::{Deserialize, Serialize};

/// Which storage system is a chunk located in?
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
pub enum ChunkStorage {
    /// The chunk is still open for new writes, in the Mutable Buffer
    OpenMutableBuffer,

    /// The chunk is no longer open for writes, in the Mutable Buffer
    ClosedMutableBuffer,

    /// The chunk is in the Read Buffer (where it can not be mutated)
    ReadBuffer,

    /// The chunk is stored in Object Storage (where it can not be mutated)
    ObjectStore,
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone, Serialize, Deserialize)]
/// Represents metadata about a chunk in a database.
/// A chunk can contain one or more tables.
pub struct ChunkSummary {
    /// The partitition key of this chunk
    pub partition_key: Arc<String>,

    /// The id of this chunk
    pub id: u32,

    /// How is this chunk stored?
    pub storage: ChunkStorage,

    /// The total estimated size of this chunk, in bytes
    pub estimated_bytes: usize,
}

/// Conversion code to management API chunk structure
impl From<ChunkSummary> for management::Chunk {
    fn from(summary: ChunkSummary) -> Self {
        let ChunkSummary {
            partition_key,
            id,
            storage,
            estimated_bytes,
        } = summary;

        let storage: management::ChunkStorage = storage.into();
        let storage = storage.into(); // convert to i32

        let estimated_bytes = estimated_bytes as u64;

        let partition_key = match Arc::try_unwrap(partition_key) {
            // no one else has a reference so take the string
            Ok(partition_key) => partition_key,
            // some other refernece exists to this string, so clone it
            Err(partition_key) => partition_key.as_ref().clone(),
        };

        Self {
            partition_key,
            id,
            storage,
            estimated_bytes,
        }
    }
}

impl From<ChunkStorage> for management::ChunkStorage {
    fn from(storage: ChunkStorage) -> Self {
        match storage {
            ChunkStorage::OpenMutableBuffer => Self::OpenMutableBuffer,
            ChunkStorage::ClosedMutableBuffer => Self::ClosedMutableBuffer,
            ChunkStorage::ReadBuffer => Self::ReadBuffer,
            ChunkStorage::ObjectStore => Self::ObjectStore,
        }
    }
}

/// Conversion code from management API chunk structure
impl TryFrom<management::Chunk> for ChunkSummary {
    type Error = FieldViolation;

    fn try_from(proto: management::Chunk) -> Result<Self, Self::Error> {
        // Use prost enum conversion
        let storage = proto.storage().scope("storage")?;

        let management::Chunk {
            partition_key,
            id,
            estimated_bytes,
            ..
        } = proto;

        let estimated_bytes = estimated_bytes as usize;
        let partition_key = Arc::new(partition_key);

        Ok(Self {
            partition_key,
            id,
            storage,
            estimated_bytes,
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
            management::ChunkStorage::ObjectStore => Ok(Self::ObjectStore),
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
            id: 42,
            estimated_bytes: 1234,
            storage: management::ChunkStorage::ObjectStore.into(),
        };

        let summary = ChunkSummary::try_from(proto).expect("conversion successful");
        let expected = ChunkSummary {
            partition_key: Arc::new("foo".to_string()),
            id: 42,
            estimated_bytes: 1234,
            storage: ChunkStorage::ObjectStore,
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
            partition_key: Arc::new("foo".to_string()),
            id: 42,
            estimated_bytes: 1234,
            storage: ChunkStorage::ObjectStore,
        };

        let proto = management::Chunk::try_from(summary).expect("conversion successful");

        let expected = management::Chunk {
            partition_key: "foo".to_string(),
            id: 42,
            estimated_bytes: 1234,
            storage: management::ChunkStorage::ObjectStore.into(),
        };

        assert_eq!(
            proto, expected,
            "Actual:\n\n{:?}\n\nExpected:\n\n{:?}\n\n",
            proto, expected
        );
    }
}
