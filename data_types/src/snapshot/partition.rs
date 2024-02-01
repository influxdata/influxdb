//! Snapshot definition for partitions

use crate::snapshot::list::MessageList;
use crate::snapshot::mask::{BitMask, BitMaskBuilder};
use crate::{
    ColumnId, ColumnSet, CompactionLevelProtoError, NamespaceId, ObjectStoreId, ParquetFile,
    ParquetFileId, Partition, PartitionHashId, PartitionHashIdError, PartitionId,
    SkippedCompaction, SortKeyIds, TableId, Timestamp,
};
use bytes::Bytes;
use generated_types::influxdata::iox::{
    catalog_cache::v1 as proto, skipped_compaction::v1 as skipped_compaction_proto,
};
use snafu::{OptionExt, ResultExt, Snafu};

/// Error for [`PartitionSnapshot`]
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
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

/// A snapshot of a partition
#[derive(Debug, Clone)]
pub struct PartitionSnapshot {
    /// The [`NamespaceId`]
    namespace_id: NamespaceId,
    /// The [`TableId`]
    table_id: TableId,
    /// The [`PartitionId`]
    partition_id: PartitionId,
    /// The [`PartitionHashId`]
    partition_hash_id: Option<PartitionHashId>,
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

        let files = files
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
                }
            })
            .collect::<Vec<_>>();

        Ok(Self {
            generation,
            columns,
            namespace_id,
            partition_id: partition.id,
            partition_hash_id: partition.hash_id().cloned(),
            key: partition.partition_key.as_bytes().to_vec().into(),
            files: MessageList::encode(&files).context(FileEncodeSnafu)?,
            sort_key: partition.sort_key_ids().cloned().unwrap_or_default(),
            table_id: partition.table_id,
            new_file_at: partition.new_file_at,
            skipped_compaction: skipped_compaction.map(|sc| sc.into()),
        })
    }

    /// Create a new [`PartitionSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Partition, generation: u64) -> Self {
        let table_id = TableId::new(proto.table_id);
        let partition_hash_id = proto
            .partition_hash_id
            .then(|| PartitionHashId::from_raw(table_id, proto.key.as_ref()));

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
    pub fn partition_hash_id(&self) -> Option<&PartitionHashId> {
        self.partition_hash_id.as_ref()
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
        })
    }

    /// Returns an iterator over the files in this snapshot
    pub fn files(&self) -> impl Iterator<Item = Result<ParquetFile>> + '_ {
        (0..self.files.len()).map(|idx| self.file(idx))
    }

    /// Returns the [`Partition`] for this snapshot
    pub fn partition(&self) -> Result<Partition> {
        let key = std::str::from_utf8(&self.key).context(PartitionKeySnafu)?;
        Ok(Partition::new_catalog_only(
            self.partition_id,
            self.partition_hash_id.clone(),
            self.table_id,
            key.into(),
            self.sort_key.clone(),
            self.new_file_at,
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
}

impl From<PartitionSnapshot> for proto::Partition {
    fn from(value: PartitionSnapshot) -> Self {
        Self {
            key: value.key,
            files: Some(value.files.into()),
            namespace_id: value.namespace_id.get(),
            table_id: value.table_id.get(),
            partition_id: value.partition_id.get(),
            partition_hash_id: value.partition_hash_id.is_some(),
            column_ids: value.columns.iter().map(|x| x.get()).collect(),
            sort_key_ids: value.sort_key.iter().map(|x| x.get()).collect(),
            new_file_at: value.new_file_at.map(|x| x.get()),
            skipped_compaction: value.skipped_compaction,
        }
    }
}
