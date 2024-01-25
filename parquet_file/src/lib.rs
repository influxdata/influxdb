//! Parquet file generation, storage, and metadata implementations.

#![deny(rustdoc::broken_intra_doc_links, rustdoc::bare_urls, rust_2018_idioms)]
#![warn(
    missing_copy_implementations,
    missing_debug_implementations,
    clippy::explicit_iter_loop,
    // See https://github.com/influxdata/influxdb_iox/pull/1671
    clippy::future_not_send,
    clippy::use_self,
    clippy::clone_on_ref_ptr,
    unreachable_pub,
    missing_docs,
    clippy::todo,
    clippy::dbg_macro,
    unused_crate_dependencies
)]
#![allow(clippy::missing_docs_in_private_items)]

use std::{path::PathBuf, str::FromStr};

// Workaround for "unused crate" lint false positives.
use workspace_hack as _;

pub mod chunk;
pub mod metadata;
pub mod serialize;
pub mod storage;
pub mod writer;

use data_types::{
    NamespaceId, ObjectStoreId, ParquetFile, ParquetFileParams, PartitionKey, TableId,
    TransitionPartitionId,
};
use object_store::path::Path;

/// Location of a Parquet file within a namespace's object store.
/// The exact format is an implementation detail and is subject to change.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct ParquetFilePath {
    namespace_id: NamespaceId,
    table_id: TableId,
    partition_id: TransitionPartitionId,
    object_store_id: ObjectStoreId,
}

impl ParquetFilePath {
    /// Create parquet file path relevant for the storage layout.
    pub fn new(
        namespace_id: NamespaceId,
        table_id: TableId,
        partition_id: &TransitionPartitionId,
        object_store_id: ObjectStoreId,
    ) -> Self {
        Self {
            namespace_id,
            table_id,
            partition_id: partition_id.clone(),
            object_store_id,
        }
    }

    /// Get object-store path.
    pub fn object_store_path(&self) -> Path {
        let Self {
            namespace_id,
            table_id,
            partition_id,
            object_store_id,
        } = self;
        Path::from_iter([
            namespace_id.to_string().as_str(),
            table_id.to_string().as_str(),
            partition_id.to_string().as_str(),
            &format!("{object_store_id}.parquet"),
        ])
    }

    /// Get object store ID.
    pub fn object_store_id(&self) -> ObjectStoreId {
        self.object_store_id
    }

    /// Set new object store ID.
    pub fn with_object_store_id(self, object_store_id: ObjectStoreId) -> Self {
        Self {
            object_store_id,
            ..self
        }
    }
}

impl From<&Self> for ParquetFilePath {
    fn from(borrowed: &Self) -> Self {
        borrowed.clone()
    }
}

impl From<(&TransitionPartitionId, &crate::metadata::IoxMetadata)> for ParquetFilePath {
    fn from((partition_id, m): (&TransitionPartitionId, &crate::metadata::IoxMetadata)) -> Self {
        Self {
            namespace_id: m.namespace_id,
            table_id: m.table_id,
            partition_id: partition_id.clone(),
            object_store_id: m.object_store_id,
        }
    }
}

impl From<&ParquetFile> for ParquetFilePath {
    fn from(f: &ParquetFile) -> Self {
        Self {
            namespace_id: f.namespace_id,
            table_id: f.table_id,
            partition_id: TransitionPartitionId::from_parts(
                f.partition_id,
                f.partition_hash_id.clone(),
            ),
            object_store_id: f.object_store_id,
        }
    }
}

impl From<&ParquetFileParams> for ParquetFilePath {
    fn from(f: &ParquetFileParams) -> Self {
        let partition_id =
            TransitionPartitionId::from_parts(f.partition_id, f.partition_hash_id.clone());

        Self {
            partition_id,
            namespace_id: f.namespace_id,
            table_id: f.table_id,
            object_store_id: f.object_store_id,
        }
    }
}

impl TryFrom<&String> for ParquetFilePath {
    type Error = object_store::path::Error;

    fn try_from(path: &String) -> Result<Self, Self::Error> {
        let mut parts = path.split(object_store::path::DELIMITER);

        let namespace_id = parts
            .next()
            .ok_or(Self::Error::EmptySegment {
                path: path.to_owned(),
            })?
            .parse::<i64>()
            .map_err(|_| Self::Error::InvalidPath {
                path: PathBuf::from(path.to_owned()),
            })?;

        let table_id = parts
            .next()
            .ok_or(Self::Error::EmptySegment {
                path: path.to_owned(),
            })?
            .parse::<i64>()
            .map_err(|_| Self::Error::InvalidPath {
                path: path.clone().into(),
            })?;
        let table_id = TableId::new(table_id);

        let partition_id = parts.next().ok_or(Self::Error::EmptySegment {
            path: path.to_owned(),
        })?;
        let partition_key = PartitionKey::from(partition_id);

        let object_store_id = parts.next().ok_or(Self::Error::EmptySegment {
            path: path.to_owned(),
        })?; // uuid.parquet
        let object_store_id =
            object_store_id
                .split('.')
                .next()
                .ok_or(Self::Error::EmptySegment {
                    path: path.to_owned(),
                })?;

        Ok(Self {
            namespace_id: NamespaceId::new(namespace_id),
            table_id,
            partition_id: TransitionPartitionId::new(table_id, &partition_key),
            object_store_id: ObjectStoreId::from_str(object_store_id).map_err(|_| {
                Self::Error::InvalidPath {
                    path: path.clone().into(),
                }
            })?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use data_types::{PartitionId, PartitionKey, TransitionPartitionId};
    use uuid::Uuid;

    #[test]
    fn parquet_file_absolute_dirs_and_file_path_database_partition_ids() {
        let pfp = ParquetFilePath::new(
            NamespaceId::new(1),
            TableId::new(2),
            &TransitionPartitionId::Deprecated(PartitionId::new(4)),
            ObjectStoreId::from_uuid(Uuid::nil()),
        );
        let path = pfp.object_store_path();
        assert_eq!(
            path.to_string(),
            "1/2/4/00000000-0000-0000-0000-000000000000.parquet",
        );
    }

    #[test]
    fn parquet_file_absolute_dirs_and_file_path_deterministic_partition_ids() {
        let table_id = TableId::new(2);
        let pfp = ParquetFilePath::new(
            NamespaceId::new(1),
            table_id,
            &TransitionPartitionId::new(table_id, &PartitionKey::from("hello there")),
            ObjectStoreId::from_uuid(Uuid::nil()),
        );
        let path = pfp.object_store_path();
        assert_eq!(
            path.to_string(),
            "1/2/d10f045c8fb5589e1db57a0ab650175c422310a1474b4de619cc2ded48f65b81\
            /00000000-0000-0000-0000-000000000000.parquet",
        );
    }

    #[test]
    fn parquet_file_path_parsed_from_object_store_path() {
        let object_store_id = uuid::Uuid::new_v4();

        // valid
        let path = format!("1/2/4/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Ok(res) if res == ParquetFilePath::new(
                NamespaceId::new(1),
                TableId::new(2),
                &TransitionPartitionId::new(
                    TableId::new(2),
                    &PartitionKey::from("4"),
                ),
                ObjectStoreId::from_uuid(object_store_id),
            ),
            "should parse valid path, instead found {:?}", pfp
        );

        // namespace_id errors
        let path = format!("2/4/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::EmptySegment { .. }),
            "should error when missing part, instead found {:?}", pfp
        );
        let path = format!("bad/2/4/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::InvalidPath { .. }),
            "should error when invalid namespace_id, instead found {:?}", pfp
        );

        // table_id errors
        let path = format!("1/4/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::EmptySegment { .. }),
            "should error when missing part, instead found {:?}", pfp
        );
        let path = format!("1/bad/4/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::InvalidPath { .. }),
            "should error when invalid table_id, instead found {:?}", pfp
        );

        // namespace_id errors
        let path = format!("2/4/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::EmptySegment { .. }),
            "should error when missing part, instead found {:?}", pfp
        );
        let path = format!("bad/2/4/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::InvalidPath { .. }),
            "should error when invalid namespace_id, instead found {:?}", pfp
        );

        // partition_id errors
        let path = format!("1/2/{}.parquet", object_store_id);
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::EmptySegment { .. }),
            "should error when missing part, instead found {:?}", pfp
        );

        // object_store_id errors
        let path = "1/2/4".to_string();
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::EmptySegment { .. }),
            "should error when missing part, instead found {:?}", pfp
        );
        let path = "1/2/4/bad".to_string();
        let pfp = ParquetFilePath::try_from(&path);
        assert_matches!(
            pfp,
            Err(e) if matches!(e, object_store::path::Error::InvalidPath { .. }),
            "should error when invalid object_store_id, instead found {:?}", pfp
        );
    }
}
