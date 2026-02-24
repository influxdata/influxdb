//! Snapshot definition for namespaces
use bytes::Bytes;
use generated_types::influxdata::iox::catalog_cache::v1 as proto;
use generated_types::influxdata::iox::partition_template::v1::PartitionTemplate;
use snafu::{ResultExt, Snafu};
use std::time::Duration;

use crate::{
    Namespace, NamespaceId, NamespacePartitionTemplateOverride, NamespaceVersion,
    ServiceLimitError, Table, TableId, Timestamp,
};

use super::{
    hash::{HashBuckets, HashBucketsEncoder},
    list::{GetId, MessageList, SortedById},
};

/// Error for [`NamespaceSnapshot`]
#[derive(Debug, Snafu)]
#[expect(missing_docs)]
pub enum Error {
    #[snafu(display("Invalid table name: {source}"))]
    NamespaceName { source: std::str::Utf8Error },

    #[snafu(display("Invalid max tables: {source}"))]
    MaxTables { source: ServiceLimitError },

    #[snafu(display("Invalid max columns per table: {source}"))]
    MaxColumnsPerTable { source: ServiceLimitError },

    #[snafu(display("Invalid partition template: {source}"))]
    PartitionTemplate {
        source: crate::partition_template::ValidationError,
    },

    #[snafu(display("Error encoding tables: {source}"))]
    TableEncode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error decoding tables: {source}"))]
    TableDecode {
        source: crate::snapshot::list::Error,
    },

    #[snafu(display("Error decoding table names: {source}"))]
    TableNamesDecode {
        source: crate::snapshot::hash::Error,
    },

    #[snafu(display(
        "Table name hash lookup resulted in out of bounds. Wanted index {wanted} but there are only {entries} entries."
    ))]
    TableNameHashOutOfBounds { wanted: usize, entries: usize },
}

/// Result for [`NamespaceSnapshot`]
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl GetId for proto::NamespaceTable {
    fn id(&self) -> i64 {
        self.id
    }
}

/// A snapshot of a namespace
#[derive(Debug, Clone)]
pub struct NamespaceSnapshot {
    id: NamespaceId,
    name: Bytes,
    retention_period_ns: Option<i64>,
    max_tables: i32,
    max_columns_per_table: i32,
    deleted_at: Option<Timestamp>,
    partition_template: Option<PartitionTemplate>,
    router_version: NamespaceVersion,
    tables: MessageList<proto::NamespaceTable>,
    table_names: HashBuckets,
    generation: u64,
    created_at: Option<Timestamp>,
}

impl NamespaceSnapshot {
    /// Create a new [`NamespaceSnapshot`] from the provided state
    pub fn encode(
        namespace: Namespace,
        tables: impl IntoIterator<Item = Table>,
        generation: u64,
    ) -> Result<Self> {
        let tables: SortedById<_> = tables
            .into_iter()
            .map(|t| proto::NamespaceTable {
                id: t.id.get(),
                name: t.name.into(),
                deleted_at: t.deleted_at.map(|ts| ts.get()),
            })
            .collect();

        let mut table_names = HashBucketsEncoder::new(tables.len());
        for (index, table) in tables.iter().enumerate() {
            // exclude soft-deleted entries from lookup-by-name
            if table.deleted_at.is_none() {
                table_names.push(&table.name, index as u32);
            }
        }

        Ok(Self {
            id: namespace.id,
            name: namespace.name.into(),
            retention_period_ns: namespace.retention_period_ns,
            max_tables: namespace.max_tables.get_i32(),
            max_columns_per_table: namespace.max_columns_per_table.get_i32(),
            deleted_at: namespace.deleted_at,
            partition_template: namespace.partition_template.as_proto().cloned(),
            router_version: namespace.router_version,
            tables: MessageList::encode(tables).context(TableEncodeSnafu)?,
            table_names: table_names.finish(),
            generation,
            created_at: namespace.created_at,
        })
    }

    /// Create a new [`NamespaceSnapshot`] from a `proto` and generation
    pub fn decode(proto: proto::Namespace, generation: u64) -> Result<Self> {
        Ok(Self {
            id: NamespaceId::new(proto.id),
            name: proto.name,
            retention_period_ns: proto.retention_period_ns,
            max_tables: proto.max_tables,
            max_columns_per_table: proto.max_columns_per_table,
            deleted_at: proto.deleted_at.map(Timestamp::new),
            partition_template: proto.partition_template,
            router_version: NamespaceVersion::new(proto.router_version),
            tables: MessageList::from(proto.tables.unwrap_or_default()),
            table_names: proto
                .table_names
                .unwrap_or_default()
                .try_into()
                .context(TableNamesDecodeSnafu)?,
            generation,
            created_at: proto.created_at.map(Timestamp::new),
        })
    }

    /// Get namespace.
    pub fn namespace(&self) -> Result<Namespace> {
        let name = std::str::from_utf8(&self.name)
            .context(NamespaceNameSnafu)?
            .to_owned();
        let max_tables = self.max_tables.try_into().context(MaxTablesSnafu)?;
        let max_columns_per_table = self
            .max_columns_per_table
            .try_into()
            .context(MaxColumnsPerTableSnafu)?;
        let partition_template = match self.partition_template.clone() {
            Some(t) => t.try_into().context(PartitionTemplateSnafu)?,
            None => NamespacePartitionTemplateOverride::const_default(),
        };

        Ok(Namespace {
            id: self.id,
            name,
            retention_period_ns: self.retention_period_ns,
            max_tables,
            max_columns_per_table,
            deleted_at: self.deleted_at,
            partition_template,
            router_version: self.router_version,
            created_at: self.created_at,
        })
    }

    /// Returns an iterator of the [`NamespaceSnapshotTable`]s in this namespace
    pub fn tables(&self) -> impl Iterator<Item = Result<NamespaceSnapshotTable>> + '_ {
        (0..self.tables.len()).map(|idx| {
            let t = self.tables.get(idx).context(TableDecodeSnafu)?;
            Ok(t.into())
        })
    }

    /// Look up a [`NamespaceSnapshotTable`] by `TableId` using binary search of the list of
    /// tables. _Does_ include soft-deleted entries.
    ///
    /// Hard-deleted tables may still appear in the table cache, but should NOT appear in
    /// the namespace snapshot's tables, so this method must be used to check actual presence or
    /// absence before looking up additional table information in the table cache.
    ///
    /// # Performance
    ///
    /// This method decodes each record the binary search needs to check, so may not be appropriate
    /// for performance-sensitive use cases.
    pub fn lookup_table_by_id(&self, id: TableId) -> Result<Option<NamespaceSnapshotTable>> {
        // This requires that the tables are sorted by ID, which `encode` does.
        Ok(self
            .tables
            .get_by_id(id.get())
            .context(TableDecodeSnafu)?
            .map(|t| t.into()))
    }

    /// Lookup a [`NamespaceSnapshotTable`] by name. Does not include deleted entries.
    pub fn lookup_table_by_name(&self, name: &str) -> Result<Option<NamespaceSnapshotTable>> {
        for idx in self.table_names.lookup(name.as_bytes()) {
            let table = self.tables.get(idx).context(TableDecodeSnafu)?;
            if table.name == name.as_bytes() {
                return Ok(Some(table.into()));
            }
        }

        Ok(None)
    }

    /// Returns the generation of this snapshot
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Get namespace ID.
    pub fn namespace_id(&self) -> NamespaceId {
        self.id
    }

    /// Returns the retention period if any
    pub fn retention_period(&self) -> Option<Duration> {
        self.retention_period_ns
            .map(|x| Duration::from_nanos(x as _))
    }

    /// When this namespace was deleted if any
    pub fn deleted_at(&self) -> Option<Timestamp> {
        self.deleted_at
    }

    /// Returns the partition template if any
    pub fn partition_template(&self) -> Option<&PartitionTemplate> {
        self.partition_template.as_ref()
    }

    /// Returns the maximum number of tables allowed in this namespace
    pub fn max_tables(&self) -> i32 {
        self.max_tables
    }

    /// Returns the maximum number of columns allowed per table in this namespace
    pub fn max_columns_per_table(&self) -> i32 {
        self.max_columns_per_table
    }
}

/// Table information stored within [`NamespaceSnapshot`]
#[derive(Debug)]
pub struct NamespaceSnapshotTable {
    id: TableId,
    name: Bytes,
    deleted_at: Option<Timestamp>,
}

impl NamespaceSnapshotTable {
    /// Returns the [`TableId`] for this table
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Returns the name for this table
    pub fn name(&self) -> &[u8] {
        &self.name
    }

    /// Returns the timestamp when the table was marked for deletion
    pub fn deleted_at(&self) -> Option<Timestamp> {
        self.deleted_at
    }
}

impl From<proto::NamespaceTable> for NamespaceSnapshotTable {
    fn from(value: proto::NamespaceTable) -> Self {
        Self {
            id: TableId::new(value.id),
            name: value.name,
            deleted_at: value.deleted_at.map(Timestamp::new),
        }
    }
}

impl From<NamespaceSnapshot> for proto::Namespace {
    fn from(value: NamespaceSnapshot) -> Self {
        Self {
            tables: Some(value.tables.into()),
            table_names: Some(value.table_names.into()),
            deleted_at: value.deleted_at.map(|d| d.get()),
            id: value.id.get(),
            name: value.name,
            retention_period_ns: value.retention_period_ns,
            max_tables: value.max_tables,
            max_columns_per_table: value.max_columns_per_table,
            partition_template: value.partition_template,
            router_version: value.router_version.get(),
            created_at: value.created_at.map(|t| t.get()),
        }
    }
}
