//! Legacy `ColumnId`-based APIs for crates that depend on `ColumnId` as the unique column
//! identifier, i.e., for Parquet. This wraps the v3 `TableDefinition` with a compatibility layer.

use crate::resource::CatalogResource;
use influxdb3_id::{ColumnId, ColumnIdentifier, TableId};
use schema::InfluxColumnType;
use std::cell::OnceCell;
use std::fmt::Debug;
use std::ops::Index;
use std::sync::Arc;

/// A wrapper for [super::schema::table::TableDefinition] that provides
/// `ColumnId`-based APIs.
#[derive(Clone, Debug)]
pub struct TableDefinition {
    inner: Arc<super::schema::table::TableDefinition>,
    pub table_id: TableId,
    pub table_name: Arc<str>,
    pub columns: ColumnSet,
    pub series_key: SeriesKey,
}

impl TableDefinition {
    pub fn new(inner: Arc<super::schema::table::TableDefinition>) -> Self {
        let table_id = inner.table_id;
        let table_name = Arc::clone(&inner.table_name);
        let columns = ColumnSet(Arc::clone(&inner));
        let series_key = SeriesKey(Arc::clone(&inner), OnceCell::new());
        Self {
            inner,
            table_id,
            table_name,
            columns,
            series_key,
        }
    }

    pub fn inner(&self) -> Arc<super::schema::table::TableDefinition> {
        Arc::clone(&self.inner)
    }

    pub fn num_columns(&self) -> usize {
        self.inner.num_columns()
    }

    pub fn column_definition(&self, name: impl AsRef<str>) -> Option<ColumnDefinition> {
        self.inner
            .column_definition(name.as_ref())
            .map(ColumnDefinition::from)
    }

    pub fn index_column_ids(&self) -> Vec<ColumnId> {
        self.inner
            .tag_columns
            .repo
            .values()
            .filter_map(|tc| tc.column_id)
            .collect()
    }

    pub fn column_definition_by_id(&self, column_id: &ColumnId) -> Option<ColumnDefinition> {
        self.inner
            .columns
            .get_by_ord_id(column_id)
            .map(ColumnDefinition::from)
    }

    pub fn column_name_to_id(&self, column_name: impl AsRef<str>) -> Option<ColumnId> {
        self.inner.columns.name_to_ord_id(column_name.as_ref())
    }

    pub fn column_name_to_id_unchecked(&self, column_name: impl AsRef<str>) -> ColumnId {
        self.inner
            .columns
            .name_to_ord_id(column_name.as_ref())
            .unwrap()
    }

    pub fn column_id_to_name(&self, column_id: &ColumnId) -> Option<Arc<str>> {
        self.inner.columns.ord_id_to_name(column_id)
    }

    /// Map the `ColumnIdentifier`s to `ColumnId`s
    pub fn ids_to_column_ids(
        &self,
        idents: &[ColumnIdentifier],
    ) -> impl Iterator<Item = &ColumnId> {
        idents.iter().filter_map(|id| {
            self.inner
                .columns
                .get_by_id(id)
                .and_then(|c| c.ord_id_ref())
        })
    }
}

/// A wrapper that lazily initializes a vector to remap a series key
/// from [TagId][influxdb3_id::TagId] to [ColumnId].
#[derive(Clone, Debug)]
pub struct SeriesKey(
    Arc<super::schema::table::TableDefinition>,
    OnceCell<Vec<ColumnId>>,
);

impl SeriesKey {
    pub fn len(&self) -> usize {
        self.0.series_key.len()
    }

    pub fn iter(&self) -> core::slice::Iter<'_, ColumnId> {
        self.get_series_key().iter()
    }

    pub fn is_empty(&self) -> bool {
        self.0.series_key.is_empty()
    }

    fn get_series_key(&self) -> &Vec<ColumnId> {
        self.1.get_or_init(|| {
            self.0
                .series_key
                .iter()
                .filter_map(|id| self.0.tag_columns.get_by_id(id).unwrap().column_id)
                .collect()
        })
    }
}

impl Index<usize> for SeriesKey {
    type Output = ColumnId;

    fn index(&self, index: usize) -> &Self::Output {
        self.get_series_key().index(index)
    }
}

/// A wrapper for the `columns` field of [TableDefinition] to provide compatible APIs
/// for clients dependent on [ColumnId] rather than [ColumnIdentifier].
#[derive(Clone, Debug)]
pub struct ColumnSet(Arc<super::schema::table::TableDefinition>);

impl ColumnSet {
    pub fn get_by_id(&self, id: &ColumnId) -> Option<ColumnDefinition> {
        self.0.columns.get_by_ord_id(id).map(ColumnDefinition::from)
    }

    pub fn get_by_name(&self, name: &str) -> Option<ColumnDefinition> {
        self.0.columns.get_by_name(name).map(ColumnDefinition::from)
    }

    pub fn name_to_id(&self, column_name: impl AsRef<str>) -> Option<ColumnId> {
        self.0.columns.name_to_ord_id(column_name.as_ref())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ColumnId, ColumnDefinition)> {
        self.0
            .columns
            .repo
            .values()
            .filter_map(|def| def.ord_id_ref().map(|ord| (ord, def.into())))
    }

    pub fn resource_iter(&self) -> impl Iterator<Item = ColumnDefinition> {
        self.0.columns.repo.values().map(Into::into)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct ColumnDefinition {
    /// Unique identifier of the column in the catalog
    pub id: ColumnId,
    /// User-provided unique name for the column
    pub name: Arc<str>,
    /// Influx type of the column
    pub data_type: InfluxColumnType,
    /// Whether this column can hold `NULL` values
    pub nullable: bool,
}

impl CatalogResource for ColumnDefinition {
    type Identifier = ColumnId;

    const CATEGORY: &'static str = "columns";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl From<super::schema::column::ColumnDefinition> for ColumnDefinition {
    fn from(value: super::schema::column::ColumnDefinition) -> Self {
        (&value).into()
    }
}

impl From<&super::schema::column::ColumnDefinition> for ColumnDefinition {
    fn from(value: &super::schema::column::ColumnDefinition) -> Self {
        match value {
            super::schema::column::ColumnDefinition::Timestamp(c) => Self {
                id: c
                    .column_id
                    .expect("legacy ColumnDefinition requires column_id"),
                name: Arc::clone(&c.name),
                data_type: InfluxColumnType::Timestamp,
                nullable: false,
            },
            super::schema::column::ColumnDefinition::Tag(c) => Self {
                id: c
                    .column_id
                    .expect("legacy ColumnDefinition requires column_id"),
                name: Arc::clone(&c.name),
                data_type: InfluxColumnType::Tag,
                nullable: true,
            },
            super::schema::column::ColumnDefinition::Field(c) => Self {
                id: c
                    .column_id
                    .expect("legacy ColumnDefinition requires column_id"),
                name: Arc::clone(&c.name),
                data_type: InfluxColumnType::Field(c.data_type),
                nullable: true,
            },
        }
    }
}

impl super::schema::database::DatabaseSchema {
    pub fn legacy_table_definition(&self, table_name: impl AsRef<str>) -> Option<TableDefinition> {
        self.table_definition(table_name).map(TableDefinition::new)
    }

    pub fn legacy_table_definition_by_id(&self, table_id: &TableId) -> Option<TableDefinition> {
        self.table_definition_by_id(table_id)
            .map(TableDefinition::new)
    }
}

impl super::schema::column::ColumnDefinition {
    /// Return a reference to the column ordinal ID.
    fn ord_id_ref(&self) -> Option<&ColumnId> {
        match self {
            Self::Timestamp(v) => v.column_id.as_ref(),
            Self::Tag(v) => v.column_id.as_ref(),
            Self::Field(v) => v.column_id.as_ref(),
        }
    }
}
