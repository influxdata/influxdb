use std::{hash::BuildHasherDefault, sync::Arc};

use ahash::RandomState as AHashBuilder;
use indexmap::IndexMap;
use influxdb3_id::{
    CatalogId, ColumnId, ColumnIdentifier, FieldFamilyId, FieldId, FieldIdentifier, TagId,
};
use rustc_hash::FxHasher;
use schema::{InfluxColumnType, InfluxFieldType};
use serde::{Deserialize, Serialize};

use crate::{
    CatalogError, Repository, Result,
    catalog::{BiHashMap, TIME_COLUMN_NAME},
    resource::CatalogResource,
};

#[derive(Debug, Eq, PartialEq, Clone)]
pub enum ColumnDefinition {
    Timestamp(Arc<TimestampColumn>),
    Tag(Arc<TagColumn>),
    Field(Arc<FieldColumn>),
}

impl ColumnDefinition {
    pub fn timestamp(column_id: impl Into<ColumnId>) -> Self {
        Self::Timestamp(Arc::new(TimestampColumn {
            column_id: Some(column_id.into()),
            name: TIME_COLUMN_NAME.into(),
        }))
    }

    pub fn tag(
        id: impl Into<TagId>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
    ) -> Self {
        Self::Tag(Arc::new(TagColumn::new(id, column_id, name)))
    }

    pub fn field(
        id: impl Into<FieldIdentifier>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
        data_type: InfluxFieldType,
    ) -> Self {
        Self::Field(Arc::new(FieldColumn::new(id, column_id, name, data_type)))
    }

    pub fn as_field(&self) -> Option<&FieldColumn> {
        match self {
            Self::Field(v) => Some(v),
            _ => None,
        }
    }
}

impl ColumnDefinition {
    /// Return the identifier of the column.
    pub fn id(&self) -> ColumnIdentifier {
        match self {
            Self::Timestamp(_) => ColumnIdentifier::Timestamp,
            Self::Tag(v) => ColumnIdentifier::Tag(v.id),
            Self::Field(v) => ColumnIdentifier::Field(v.id),
        }
    }

    /// Return the legacy column ID, when one is assigned.
    ///
    /// Columns added to a PachaTree-mode table after `u16::MAX` ids have been assigned will
    /// return `None`.
    pub fn ord_id(&self) -> Option<ColumnId> {
        match self {
            Self::Timestamp(v) => v.column_id,
            Self::Tag(v) => v.column_id,
            Self::Field(v) => v.column_id,
        }
    }

    /// Return the name of the column.
    pub fn name(&self) -> Arc<str> {
        match self {
            Self::Timestamp(v) => Arc::clone(&v.name),
            Self::Tag(v) => Arc::clone(&v.name),
            Self::Field(v) => Arc::clone(&v.name),
        }
    }

    /// Return the InfluxDB type of the column.
    pub fn column_type(&self) -> InfluxColumnType {
        match self {
            Self::Timestamp(_) => InfluxColumnType::Timestamp,
            Self::Tag(_) => InfluxColumnType::Tag,
            Self::Field(v) => InfluxColumnType::Field(v.data_type),
        }
    }

    /// Returns whether the column can hold `NULL` values.
    pub fn is_nullable(&self) -> bool {
        matches!(self, Self::Field(_) | Self::Tag(_))
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TimestampColumn {
    /// See `ColumndDefinition::ord_id`
    pub column_id: Option<ColumnId>,
    pub name: Arc<str>,
}

impl TimestampColumn {
    pub fn new(column_id: impl Into<ColumnId>, name: impl Into<Arc<str>>) -> Self {
        Self {
            column_id: Some(column_id.into()),
            name: name.into(),
        }
    }
}

impl From<Arc<TimestampColumn>> for ColumnDefinition {
    fn from(column: Arc<TimestampColumn>) -> Self {
        Self::Timestamp(column)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TagColumn {
    pub id: TagId,
    /// See `ColumndDefinition::ord_id`
    pub column_id: Option<ColumnId>,
    pub name: Arc<str>,
}

impl CatalogResource for TagColumn {
    type Identifier = TagId;

    const CATEGORY: &'static str = "tag_columns";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl TagColumn {
    pub fn new(
        id: impl Into<TagId>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            id: id.into(),
            column_id: Some(column_id.into()),
            name: name.into(),
        }
    }
}

impl From<Arc<TagColumn>> for ColumnDefinition {
    fn from(column: Arc<TagColumn>) -> Self {
        ColumnDefinition::Tag(column)
    }
}

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FieldColumn {
    pub id: FieldIdentifier,
    /// See `ColumndDefinition::ord_id`
    pub column_id: Option<ColumnId>,
    pub name: Arc<str>,
    pub data_type: InfluxFieldType,
}

impl CatalogResource for FieldColumn {
    type Identifier = FieldIdentifier;

    const CATEGORY: &'static str = "fields";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        Arc::clone(&self.name)
    }
}

impl FieldColumn {
    pub fn new(
        id: impl Into<FieldIdentifier>,
        column_id: impl Into<ColumnId>,
        name: impl Into<Arc<str>>,
        data_type: InfluxFieldType,
    ) -> Self {
        Self {
            id: id.into(),
            column_id: Some(column_id.into()),
            name: name.into(),
            data_type,
        }
    }
}

impl From<Arc<FieldColumn>> for ColumnDefinition {
    fn from(column: Arc<FieldColumn>) -> Self {
        ColumnDefinition::Field(column)
    }
}

#[derive(Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FieldFamilyName {
    /// A user-defined field family with a specific name
    User(Arc<str>),
    /// The Nth auto-generated field family
    Auto(u16),
}

/// Definition of a field family in the catalog for a specific table.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct FieldFamilyDefinition {
    pub id: FieldFamilyId,
    pub name: FieldFamilyName,
    pub fields: Repository<FieldId, FieldColumn>,
}

impl CatalogResource for FieldFamilyDefinition {
    type Identifier = FieldFamilyId;

    const CATEGORY: &'static str = "field_families";

    fn id(&self) -> Self::Identifier {
        self.id
    }

    fn name(&self) -> Arc<str> {
        match &self.name {
            FieldFamilyName::User(name) => Arc::clone(name),
            FieldFamilyName::Auto(v) => format!("__{v}").into(),
        }
    }
}

impl FieldFamilyDefinition {
    pub fn new(id: FieldFamilyId, name: FieldFamilyName) -> Self {
        Self {
            id,
            name,
            fields: Repository::new(),
        }
    }
}

type FxBuildHasher = BuildHasherDefault<FxHasher>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct ColumnSet {
    /// Store for items in the repository
    pub(crate) repo: IndexMap<ColumnIdentifier, ColumnDefinition, FxBuildHasher>,
    /// Bi-directional map of identifiers to names in the repository
    pub(crate) id_name_map: BiHashMap<ColumnIdentifier, Arc<str>>,
    /// Bi-directional map of identifiers to ordinal column IDs in the repository
    pub(crate) id_ord_id_map: BiHashMap<ColumnIdentifier, ColumnId>,
    /// The next available column ID to use for new columns
    pub(crate) next_id: Option<ColumnId>,
}

impl ColumnSet {
    pub fn new() -> Self {
        Self {
            repo: IndexMap::with_hasher(FxBuildHasher::default()),
            id_name_map: bimap::BiHashMap::with_hashers(
                AHashBuilder::default(),
                AHashBuilder::default(),
            ),
            id_ord_id_map: bimap::BiHashMap::with_hashers(
                AHashBuilder::default(),
                AHashBuilder::default(),
            ),
            next_id: Some(0.into()),
        }
    }

    pub fn get_and_increment_next_id(&mut self) -> Option<ColumnId> {
        let next_id = self.next_id;
        self.next_id = self.next_id.and_then(|id| id.checked_next());
        next_id
    }

    pub(crate) fn set_next_id(&mut self, id: ColumnId) {
        self.next_id = Some(id);
    }

    pub fn get_by_name(&self, name: &str) -> Option<&ColumnDefinition> {
        self.id_name_map
            .get_by_right(name)
            .and_then(|id| self.repo.get(id))
    }

    pub fn get_by_id(&self, id: &ColumnIdentifier) -> Option<&ColumnDefinition> {
        self.repo.get(id)
    }

    pub fn get_by_ord_id(&self, column_id: &ColumnId) -> Option<&ColumnDefinition> {
        self.id_ord_id_map
            .get_by_right(column_id)
            .and_then(|id| self.repo.get(id))
    }

    pub fn id_to_name(&self, id: &ColumnIdentifier) -> Option<Arc<str>> {
        self.id_name_map.get_by_left(id).cloned()
    }

    pub fn name_to_id(&self, column_name: &str) -> Option<ColumnIdentifier> {
        self.id_name_map.get_by_right(column_name).cloned()
    }

    pub fn name_to_ord_id(&self, column_name: &str) -> Option<ColumnId> {
        self.id_name_map
            .get_by_right(column_name)
            .and_then(|id| self.id_ord_id_map.get_by_left(id).copied())
    }

    pub fn ord_id_to_name(&self, column_id: &ColumnId) -> Option<Arc<str>> {
        self.id_ord_id_map
            .get_by_right(column_id)
            .and_then(|id| self.id_name_map.get_by_left(id).cloned())
    }

    pub fn id_to_ord_id(&self, id: &ColumnIdentifier) -> Option<ColumnId> {
        self.id_ord_id_map.get_by_left(id).copied()
    }

    pub fn contains_id(&self, id: &ColumnIdentifier) -> bool {
        self.repo.contains_key(id)
    }

    pub fn contains_name(&self, name: &str) -> bool {
        self.id_name_map.contains_right(name)
    }

    pub fn len(&self) -> usize {
        self.repo.len()
    }

    pub fn is_empty(&self) -> bool {
        self.repo.is_empty()
    }

    /// Check if a resource exists in the repository by `id`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn id_exists(&self, id: &ColumnIdentifier) -> bool {
        let id_in_name_map = self.id_name_map.contains_left(id);
        let id_in_col_id_map = self.id_ord_id_map.contains_left(id);
        let id_in_repo = self.repo.contains_key(id);
        assert!(
            (id_in_name_map == id_in_repo) && (!id_in_col_id_map || id_in_repo),
            "name, col_id and repository are in an inconsistent state, \
            name map: {id_in_name_map}, col_id map: {id_in_col_id_map}, in repo: {id_in_repo}"
        );
        id_in_repo
    }

    /// Check if a [ColumnDefinition] exists in the repository by `id`, `column_id` and `name`
    ///
    /// # Panics
    ///
    /// This panics if the `id` is in the id-to-name map, but not in the actual repository map, as
    /// that would be a bad state for the repository to be in.
    fn definition_exists(&self, v: &ColumnDefinition) -> bool {
        let name_in_map = self.id_name_map.contains_right(v.name().as_ref());
        let ord_id_in_map = v
            .ord_id()
            .is_none_or(|ord| self.id_ord_id_map.contains_right(&ord));
        self.id_exists(&v.id()) && name_in_map && ord_id_in_map
    }

    /// Insert a new resource to the repository
    pub fn insert(&mut self, resource: impl Into<ColumnDefinition>) -> Result<()> {
        let resource = resource.into();
        if self.definition_exists(&resource) {
            return Err(CatalogError::AlreadyExists);
        }
        let id = resource.id();
        let ord_id = resource.ord_id();
        self.id_name_map.insert(id, resource.name());
        if let Some(ord_id) = ord_id {
            self.id_ord_id_map.insert(id, ord_id);
            self.next_id = self.next_id.and_then(|id| id.checked_next());
        }
        self.repo.insert(id, resource);
        Ok(())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ColumnIdentifier, &ColumnDefinition)> {
        self.repo.iter()
    }

    pub fn resource_iter(&self) -> impl Iterator<Item = &ColumnDefinition> {
        self.repo.values()
    }
}

impl Default for ColumnSet {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum FieldFamilyMode {
    /// Fields must always be specified with a field family prefix for
    /// SQL and line protocol.
    #[default]
    Aware,
    /// Fields are never interpreted with a field family prefix, and
    /// fields are automatically assigned to the next available slot in
    /// the current auto field family.
    Auto,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum FieldDataType {
    String,
    Integer,
    UInteger,
    Float,
    Boolean,
    Binary,
}

impl From<FieldDataType> for InfluxFieldType {
    fn from(value: FieldDataType) -> Self {
        match value {
            FieldDataType::String => Self::String,
            FieldDataType::Integer => Self::Integer,
            FieldDataType::UInteger => Self::UInteger,
            FieldDataType::Float => Self::Float,
            FieldDataType::Boolean => Self::Boolean,
            FieldDataType::Binary => {
                // TODO: Update when InfluxFieldType supports Binary
                Self::String
            }
        }
    }
}

impl From<InfluxFieldType> for FieldDataType {
    fn from(value: InfluxFieldType) -> Self {
        match value {
            InfluxFieldType::String => Self::String,
            InfluxFieldType::Integer => Self::Integer,
            InfluxFieldType::UInteger => Self::UInteger,
            InfluxFieldType::Float => Self::Float,
            InfluxFieldType::Boolean => Self::Boolean,
        }
    }
}

impl From<crate::log::FieldDataType> for FieldDataType {
    fn from(value: crate::log::FieldDataType) -> Self {
        match value {
            crate::log::FieldDataType::String => Self::String,
            crate::log::FieldDataType::Integer => Self::Integer,
            crate::log::FieldDataType::UInteger => Self::UInteger,
            crate::log::FieldDataType::Float => Self::Float,
            crate::log::FieldDataType::Boolean => Self::Boolean,
            crate::log::FieldDataType::Binary => Self::Binary,
        }
    }
}

impl From<FieldDataType> for InfluxColumnType {
    fn from(value: FieldDataType) -> Self {
        InfluxColumnType::Field(value.into())
    }
}
