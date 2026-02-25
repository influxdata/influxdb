//! Snapshot definition for partitions

use crate::{
    ColumnId, ColumnSet, CompactionLevelProtoError, NamespaceId, ObjectStoreId, ParquetFile,
    ParquetFileId, ParquetFileSource, Partition, PartitionHashId, PartitionHashIdError,
    PartitionId, PartitionKey, SkippedCompaction, SortKeyIds, TableId, Timestamp,
    snapshot::{
        list::{GetId, MessageList, SortedById},
        mask::{BitMask, BitMaskBuilder},
    },
};
use bytes::Bytes;
use generated_types::influxdata::iox::{
    catalog_cache::v1 as proto, skipped_compaction::v1 as skipped_compaction_proto,
};
use snafu::{OptionExt, ResultExt, Snafu};

/// Error for [`PartitionSnapshot`]
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(display("Error decoding PartitionFile: {source}"))]
    FileDecode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error encoding ParquetFile: {source}"))]
    FileEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Missing required field {field}"))]
    RequiredField { field: &'static str },

    #[snafu(context(false))]
    CompactionLevel { source: CompactionLevelProtoError },

    #[snafu(context(false))]
    PartitionHashId { source: PartitionHashIdError },

    #[snafu(display("Invalid partition key: {source}"))]
    PartitionKey { source: std::str::Utf8Error },
}

/// Result for [`PartitionSnapshot`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl GetId for proto::PartitionFile {
    fn id(&self) -> i64 {
        self.id
    }
}

/// A snapshot of a partition
///
/// # Soft Deletion
/// This snapshot does NOT contains soft-deleted parquet files.
#[derive(Debug, Clone)]
pub struct PartitionSnapshot {
    /// The [`NamespaceId`]
    namespace_id: NamespaceId,
    /// The [`TableId`]
    table_id: TableId,
    /// The [`PartitionId`]
    partition_id: PartitionId,
    /// The [`PartitionHashId`]
    partition_hash_id: PartitionHashId,
    /// The generation of this snapshot
    generation: u64,
    /// The partition key
    key: Bytes,
    /// The files
    files: MessageList<proto::PartitionFile>,
    /// The columns for this partition
    columns: ColumnSet,
    /// The sort key ids
    sort_key: SortKeyIds,
    /// The time of a new file
    new_file_at: Option<Timestamp>,
    /// Skipped compaction.
    skipped_compaction: Option<skipped_compaction_proto::SkippedCompaction>,
    /// The time of the last cold compaction
    cold_compact_at: Option<Timestamp>,
    /// The time this Partition was created at, or `None` if this partition was created before this
    /// field existed. Not the time the snapshot was created.
    created_at: Option<Timestamp>,
    /// Estimated size in bytes of all the active files in this partition, or `None`
    /// if the partition size has not been computed yet.
    estimated_size_bytes: Option<i64>,
}

impl PartitionSnapshot {
    /// Create a new [`PartitionSnapshot`] from the provided state
    pub fn encode(
        namespace_id: NamespaceId,
        partition: Partition,
        files: Vec<ParquetFile>,
        skipped_compaction: Option<SkippedCompaction>,
        generation: u64,
    ) -> Result<Self> {
        // Iterate in reverse order as schema additions are normally additive and
        // so the later files will typically have more columns
        let columns = files.iter().rev().fold(ColumnSet::empty(), |mut acc, v| {
            acc.union(&v.column_set);
            acc
        });

        let files: SortedById<_> = files
            .into_iter()
            .map(|file| {
                let mut mask = BitMaskBuilder::new(columns.len());
                for (idx, _) in columns.intersect(&file.column_set) {
                    mask.set_bit(idx);
                }

                proto::PartitionFile {
                    id: file.id.get(),
                    object_store_uuid: Some(file.object_store_id.get_uuid().into()),
                    min_time: file.min_time.0,
                    max_time: file.max_time.0,
                    file_size_bytes: file.file_size_bytes,
                    row_count: file.row_count,
                    compaction_level: file.compaction_level as _,
                    created_at: file.created_at.0,
                    max_l0_created_at: file.max_l0_created_at.0,
                    column_mask: Some(mask.finish().into()),
                    source: file.source.map(|i| i as i32).unwrap_or_default(),
                    #[expect(deprecated)]
                    use_numeric_partition_id: Some(false),
                }
            })
            .collect();

        Ok(Self {
            generation,
            columns,
            namespace_id,
            partition_id: partition.id,
            partition_hash_id: partition.hash_id().clone(),
            key: partition.partition_key.as_bytes().to_vec().into(),
            files: MessageList::encode(files).context(FileEncodeSnafu)?,
            sort_key: partition.sort_key_ids().cloned().unwrap_or_default(),
            table_id: partition.table_id,
            new_file_at: partition.new_file_at,
            skipped_compaction: skipped_compaction.map(|sc| sc.into()),
            cold_compact_at: partition.cold_compact_at,
            created_at: partition.created_at(),
            estimated_size_bytes: partition.estimated_size_bytes,
        })
    }

    /// Create a new [`PartitionSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Partition, generation: u64) -> Self {
        let table_id = TableId::new(proto.table_id);
        let partition_hash_id = PartitionHashId::from_raw(table_id, proto.key.as_ref());

        Self {
            generation,
            table_id,
            partition_hash_id,
            key: proto.key,
            files: MessageList::from(proto.files.unwrap_or_default()),
            namespace_id: NamespaceId::new(proto.namespace_id),
            partition_id: PartitionId::new(proto.partition_id),
            columns: ColumnSet::new(proto.column_ids.into_iter().map(ColumnId::new)),
            sort_key: SortKeyIds::new(proto.sort_key_ids.into_iter().map(ColumnId::new)),
            new_file_at: proto.new_file_at.map(Timestamp::new),
            skipped_compaction: proto.skipped_compaction,
            cold_compact_at: proto.cold_compact_at.map(Timestamp::new),
            created_at: proto.created_at.map(Timestamp::new),
            estimated_size_bytes: proto.estimated_size_bytes,
        }
    }

    /// Returns the generation of this snapshot
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the [`PartitionId`]
    pub fn partition_id(&self) -> PartitionId {
        self.partition_id
    }

    /// Returns the [`PartitionHashId`] if any
    pub fn partition_hash_id(&self) -> &PartitionHashId {
        &self.partition_hash_id
    }

    /// Returns the file at index `idx`
    pub fn file(&self, idx: usize) -> Result<ParquetFile> {
        let file = self.files.get(idx).context(FileDecodeSnafu)?;

        let uuid = file.object_store_uuid.context(RequiredFieldSnafu {
            field: "object_store_uuid",
        })?;

        let column_set = match file.column_mask {
            Some(mask) => {
                let mask = BitMask::from(mask);
                ColumnSet::new(mask.set_indices().map(|idx| self.columns[idx]))
            }
            None => self.columns.clone(),
        };

        Ok(ParquetFile {
            id: ParquetFileId(file.id),
            namespace_id: self.namespace_id,
            table_id: self.table_id,
            partition_id: self.partition_id,
            partition_hash_id: self.partition_hash_id.clone(),
            object_store_id: ObjectStoreId::from_uuid(uuid.into()),
            min_time: Timestamp(file.min_time),
            max_time: Timestamp(file.max_time),
            to_delete: None,
            file_size_bytes: file.file_size_bytes,
            row_count: file.row_count,
            compaction_level: file.compaction_level.try_into()?,
            created_at: Timestamp(file.created_at),
            column_set,
            max_l0_created_at: Timestamp(file.max_l0_created_at),
            source: ParquetFileSource::from_proto(file.source),
        })
    }

    /// Returns an iterator over the files in this snapshot
    pub fn files(&self) -> impl Iterator<Item = Result<ParquetFile>> + '_ {
        (0..self.files.len()).map(|idx| self.file(idx))
    }

    fn key(&self) -> Result<PartitionKey> {
        Ok(std::str::from_utf8(&self.key)
            .context(PartitionKeySnafu)?
            .into())
    }

    /// Returns the [`Partition`] for this snapshot
    pub fn partition(&self) -> Result<Partition> {
        Ok(Partition::new_catalog_only(
            self.partition_id,
            self.table_id,
            self.key()?,
            self.sort_key.clone(),
            self.new_file_at,
            self.cold_compact_at,
            self.created_at,
            None, // max_time - not stored in snapshot (can be computed from partition key)
            self.estimated_size_bytes,
        ))
    }

    /// Returns the columns IDs
    pub fn column_ids(&self) -> &ColumnSet {
        &self.columns
    }

    /// Return skipped compaction for this partition, if any.
    pub fn skipped_compaction(&self) -> Option<SkippedCompaction> {
        self.skipped_compaction
            .as_ref()
            .cloned()
            .map(|sc| sc.into())
    }

    /// Returns the estimated size of the partition in bytes.
    pub fn estimated_size_bytes(&self) -> i64 {
        // Treat None as 0. Since this is an estimated size,
        // it is acceptable to treat partitions with None as having size 0.
        self.estimated_size_bytes.unwrap_or(0)
    }
}

impl From<PartitionSnapshot> for proto::Partition {
    fn from(value: PartitionSnapshot) -> Self {
        Self {
            key: value.key,
            files: Some(value.files.into()),
            namespace_id: value.namespace_id.get(),
            table_id: value.table_id.get(),
            partition_id: value.partition_id.get(),
            partition_hash_id: true,
            column_ids: value.columns.iter().map(|x| x.get()).collect(),
            sort_key_ids: value.sort_key.iter().map(|x| x.get()).collect(),
            new_file_at: value.new_file_at.map(|x| x.get()),
            skipped_compaction: value.skipped_compaction,
            cold_compact_at: value.cold_compact_at.map(|x| x.get()),
            created_at: value.created_at.map(|x| x.get()),
            estimated_size_bytes: value.estimated_size_bytes,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::PartitionKey;

    // Even though all partitions now have hash IDs, keep this test to ensure we can continue to
    // decode and use any cached proto that doesn't use hash IDs.
    #[expect(deprecated)]
    #[test]
    fn decode_old_cached_proto() {
        let partition_key = PartitionKey::from("arbitrary");

        // Create cached proto for three different files:
        //
        // 1. without "use_numeric_partition_id", representing proto cached before the field was
        //    added
        // 2. with "use_numeric_partition_id" = false
        // 3. with "use_numeric_partition_id" = true
        let parquet_file_missing_new_numeric_id_field_proto = proto::PartitionFile {
            id: 1,
            use_numeric_partition_id: None,

            column_mask: Default::default(),
            compaction_level: Default::default(),
            created_at: Default::default(),
            file_size_bytes: Default::default(),
            max_l0_created_at: Default::default(),
            min_time: Default::default(),
            max_time: Default::default(),
            object_store_uuid: Some(ObjectStoreId::new().get_uuid().into()),
            row_count: Default::default(),
            source: Default::default(),
        };
        let parquet_file_new_numeric_id_field_false_proto = proto::PartitionFile {
            id: 2,
            use_numeric_partition_id: Some(false),
            ..parquet_file_missing_new_numeric_id_field_proto.clone()
        };
        let parquet_file_new_numeric_id_field_true_proto = proto::PartitionFile {
            id: 3,
            use_numeric_partition_id: Some(true),
            ..parquet_file_missing_new_numeric_id_field_proto.clone()
        };
        let parquet_files = SortedById::new(vec![
            parquet_file_missing_new_numeric_id_field_proto,
            parquet_file_new_numeric_id_field_false_proto,
            parquet_file_new_numeric_id_field_true_proto,
        ]);

        let files = MessageList::encode(parquet_files).unwrap();
        let files_proto: proto::MessageList = files.into();

        // Create cached proto for two different Partitions:
        //
        // 1. Identified with a hash ID (new style)
        // 2. Identified only with a numeric ID (old style)
        //
        // and add the encoded Parquet file message list to each of them.
        let hash_id_partition_proto = proto::Partition {
            partition_hash_id: true,
            partition_id: 6,

            namespace_id: 4,
            table_id: 5,
            cold_compact_at: Default::default(),
            created_at: Default::default(),
            column_ids: Default::default(),
            files: Some(files_proto.clone()),
            key: partition_key.as_bytes().to_vec().into(),
            new_file_at: Default::default(),
            skipped_compaction: Default::default(),
            sort_key_ids: Default::default(),
            estimated_size_bytes: Default::default(),
        };
        let numeric_id_partition_proto = proto::Partition {
            partition_hash_id: false,
            partition_id: 7,
            ..hash_id_partition_proto.clone()
        };

        let decoded_hash_id_partition = PartitionSnapshot::decode(hash_id_partition_proto, 1);
        let decoded_numeric_id_partition = PartitionSnapshot::decode(numeric_id_partition_proto, 1);

        // For the Parquet file without `use_numeric_partition_id` set, it should be addressed
        // with hash ID because this should be impossible now.
        let pf0_hash_id_partition = decoded_hash_id_partition.file(0).unwrap();
        assert_eq!(
            pf0_hash_id_partition.partition_hash_id,
            decoded_hash_id_partition.partition_hash_id.clone()
        );
        let pf0_numeric_id_partition = decoded_numeric_id_partition.file(0).unwrap();
        assert_eq!(
            pf0_numeric_id_partition.partition_hash_id,
            decoded_hash_id_partition.partition_hash_id
        );

        // For the Parquet file with `use_numeric_partition_id` set to `false`, it should be
        // addressed with hash ID, regardless of how the partition is addressed.
        let pf1_hash_id_partition = decoded_hash_id_partition.file(1).unwrap();
        assert_eq!(
            pf1_hash_id_partition.partition_hash_id,
            decoded_hash_id_partition.partition_hash_id.clone()
        );
        let pf1_numeric_id_partition = decoded_numeric_id_partition.file(1).unwrap();
        assert_eq!(
            pf1_numeric_id_partition.partition_hash_id,
            PartitionHashId::new(
                decoded_numeric_id_partition.table_id,
                &decoded_numeric_id_partition.key().unwrap()
            )
        );

        // For the Parquet file with `use_numeric_partition_id` set to `true`, it should be
        // addressed with hash ID because this should be impossible now.
        let pf1_hash_id_partition = decoded_hash_id_partition.file(2).unwrap();
        assert_eq!(
            pf1_hash_id_partition.partition_hash_id,
            decoded_hash_id_partition.partition_hash_id.clone()
        );
        let pf1_numeric_id_partition = decoded_numeric_id_partition.file(2).unwrap();
        assert_eq!(
            pf1_numeric_id_partition.partition_hash_id,
            decoded_hash_id_partition.partition_hash_id.clone()
        );
    }
}
