//! Snapshot definition for tables
use crate::snapshot::list::MessageList;
use crate::{
    Column, ColumnId, ColumnTypeProtoError, NamespaceId, Partition, PartitionId, Table, TableId,
};
use bytes::Bytes;
use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use generated_types::influxdata::iox::column_type::v1::ColumnType;
use generated_types::influxdata::iox::partition_template::v1::PartitionTemplate;
use snafu::{ResultExt, Snafu};

/// Error for [`TableSnapshot`]
#[derive(Debug, Snafu)]
#[allow(missing_docs)]
pub enum Error {
    #[snafu(display("Error decoding TablePartition: {source}"))]
    PartitionDecode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error encoding TablePartition: {source}"))]
    PartitionEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error decoding TableColumn: {source}"))]
    ColumnDecode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error encoding TableColumn: {source}"))]
    ColumnEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Invalid column name: {source}"))]
    ColumnName { source: std::str::Utf8Error },

    #[snafu(display("Invalid table name: {source}"))]
    TableName { source: std::str::Utf8Error },

    #[snafu(display("Invalid partition template: {source}"))]
    PartitionTemplate {
        source: crate::partition_template::ValidationError,
    },

    #[snafu(context(false))]
    ColumnType { source: ColumnTypeProtoError },
}

/// Result for [`TableSnapshot`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

/// A snapshot of a table
#[derive(Debug, Clone)]
pub struct TableSnapshot {
    table_id: TableId,
    namespace_id: NamespaceId,
    table_name: Bytes,
    partitions: MessageList<proto::TablePartition>,
    columns: MessageList<proto::TableColumn>,
    partition_template: Option<PartitionTemplate>,
    generation: u64,
}

impl TableSnapshot {
    /// Create a new [`TableSnapshot`] from the provided state
    pub fn encode(
        table: Table,
        partitions: Vec<Partition>,
        columns: Vec<Column>,
        generation: u64,
    ) -> Result<Self> {
        let columns: Vec<_> = columns
            .into_iter()
            .map(|c| proto::TableColumn {
                id: c.id.get(),
                name: c.name.into(),
                column_type: ColumnType::from(c.column_type).into(),
            })
            .collect();

        let partitions: Vec<_> = partitions
            .into_iter()
            .map(|p| proto::TablePartition {
                id: p.id.get(),
                key: p.partition_key.as_bytes().to_vec().into(),
            })
            .collect();

        Ok(Self {
            table_id: table.id,
            namespace_id: table.namespace_id,
            table_name: table.name.into(),
            partitions: MessageList::encode(&partitions).context(PartitionEncodeSnafu)?,
            columns: MessageList::encode(&columns).context(ColumnEncodeSnafu)?,
            partition_template: table.partition_template.as_proto().cloned(),
            generation,
        })
    }

    /// Create a new [`TableSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Table, generation: u64) -> Self {
        Self {
            generation,
            table_id: TableId::new(proto.table_id),
            namespace_id: NamespaceId::new(proto.namespace_id),
            table_name: proto.table_name,
            partitions: MessageList::from(proto.partitions.unwrap_or_default()),
            columns: MessageList::from(proto.columns.unwrap_or_default()),
            partition_template: proto.partition_template,
        }
    }

    /// Returns the [`Table`] for this snapshot
    pub fn table(&self) -> Result<Table> {
        let name = std::str::from_utf8(&self.table_name).context(TableNameSnafu)?;
        let template = self
            .partition_template
            .clone()
            .try_into()
            .context(PartitionTemplateSnafu)?;

        Ok(Table {
            id: self.table_id,
            namespace_id: self.namespace_id,
            name: name.into(),
            partition_template: template,
        })
    }

    /// Returns the column by index
    pub fn column(&self, idx: usize) -> Result<Column> {
        let column = self.columns.get(idx).context(ColumnDecodeSnafu)?;
        let name = std::str::from_utf8(&column.name).context(ColumnNameSnafu)?;

        Ok(Column {
            id: ColumnId::new(column.id),
            table_id: self.table_id,
            name: name.into(),
            column_type: (column.column_type as i16).try_into()?,
        })
    }

    /// Returns an iterator of the columns in this table
    pub fn columns(&self) -> impl Iterator<Item = Result<Column>> + '_ {
        (0..self.columns.len()).map(|idx| self.column(idx))
    }

    /// Returns an iterator of the [`PartitionId`] in this table
    pub fn partitions(&self) -> impl Iterator<Item = Result<TableSnapshotPartition>> + '_ {
        (0..self.partitions.len()).map(|idx| {
            let p = self.partitions.get(idx).context(PartitionDecodeSnafu)?;
            Ok(TableSnapshotPartition {
                id: PartitionId::new(p.id),
                key: p.key,
            })
        })
    }

    /// Returns the generation of this snapshot
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Partition information stored within [`TableSnapshot`]
#[derive(Debug)]
pub struct TableSnapshotPartition {
    id: PartitionId,
    key: Bytes,
}

impl TableSnapshotPartition {
    /// Returns the [`PartitionId`] for this partition
    pub fn id(&self) -> PartitionId {
        self.id
    }

    /// Returns the partition key for this partition
    pub fn key(&self) -> &[u8] {
        &self.key
    }
}

impl From<TableSnapshot> for proto::Table {
    fn from(value: TableSnapshot) -> Self {
        Self {
            partitions: Some(value.partitions.into()),
            columns: Some(value.columns.into()),
            partition_template: value.partition_template,
            namespace_id: value.namespace_id.get(),
            table_id: value.table_id.get(),
            table_name: value.table_name,
        }
    }
}
