use crate::catalog::{
    CatalogSequenceNumber, ColumnDefinition, DatabaseSchema, InnerCatalog, NodeDefinition,
    NodeState, Repository, TableDefinition, TokenRepository,
};
use crate::log::{
    DistinctCacheDefinition, LastCacheDefinition, LastCacheTtl, LastCacheValueColumnsDef, MaxAge,
    MaxCardinality, NodeMode, TriggerDefinition, TriggerSettings, TriggerSpecificationDefinition,
};
use crate::resource::CatalogResource;
use arrow::datatypes::DataType as ArrowDataType;
use bimap::BiHashMap;
use hashbrown::HashMap;
use influxdb3_authz::{
    Actions, CrudActions, DatabaseActions, Permission, ResourceIdentifier, ResourceType, TokenInfo,
};
use influxdb3_id::{
    CatalogId, ColumnId, DbId, DistinctCacheId, LastCacheId, NodeId, SerdeVecMap, TableId, TokenId,
    TriggerId,
};
use schema::{InfluxColumnType, InfluxFieldType, TIME_DATA_TIMEZONE};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use uuid::Uuid;

pub(crate) trait Snapshot {
    type Serialized;

    fn snapshot(&self) -> Self::Serialized;
    fn from_snapshot(snap: Self::Serialized) -> Self;
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CatalogSnapshot {
    pub(crate) nodes: RepositorySnapshot<NodeId, NodeSnapshot>,
    pub(crate) databases: RepositorySnapshot<DbId, DatabaseSnapshot>,
    pub(crate) sequence: CatalogSequenceNumber,
    #[serde(default)]
    pub(crate) tokens: RepositorySnapshot<TokenId, TokenInfoSnapshot>,
    pub(crate) catalog_id: Arc<str>,
    pub(crate) catalog_uuid: Uuid,
}

impl CatalogSnapshot {
    pub(crate) fn sequence_number(&self) -> CatalogSequenceNumber {
        self.sequence
    }
}

impl Snapshot for InnerCatalog {
    type Serialized = CatalogSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            nodes: self.nodes.snapshot(),
            databases: self.databases.snapshot(),
            sequence: self.sequence,
            catalog_id: Arc::clone(&self.catalog_id),
            tokens: self.tokens.repo().snapshot(),
            catalog_uuid: self.catalog_uuid,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let repository: Repository<TokenId, TokenInfo> = Repository::from_snapshot(snap.tokens);
        let mut hash_lookup_map = BiHashMap::new();
        repository.repo.iter().for_each(|(id, info)| {
            // this clone should maybe be switched to arc?
            hash_lookup_map.insert(*id, info.hash.clone());
        });

        let token_info_repo = TokenRepository::new(repository, hash_lookup_map);
        Self {
            sequence: snap.sequence,
            catalog_id: snap.catalog_id,
            catalog_uuid: snap.catalog_uuid,
            nodes: Repository::from_snapshot(snap.nodes),
            databases: Repository::from_snapshot(snap.databases),
            tokens: token_info_repo,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct TokenInfoSnapshot {
    id: TokenId,
    name: Arc<str>,
    hash: Vec<u8>,
    created_at: i64,
    description: Option<String>,
    created_by: Option<TokenId>,
    expiry: i64,
    updated_by: Option<TokenId>,
    updated_at: Option<i64>,
    permissions: Vec<PermissionSnapshot>,
}

impl Snapshot for TokenInfo {
    type Serialized = TokenInfoSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            name: Arc::clone(&self.name),
            hash: self.hash.clone(),
            created_at: self.created_at,
            expiry: self.expiry_millis,
            created_by: self.created_by,
            updated_at: self.updated_at,
            updated_by: self.updated_by,
            description: self.description.clone(),
            permissions: self
                .permissions
                .iter()
                .map(|perm| perm.snapshot())
                .collect(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            hash: snap.hash,
            created_at: snap.created_at,
            expiry_millis: snap.expiry,
            created_by: snap.created_by,
            updated_by: snap.updated_by,
            updated_at: snap.updated_at,
            permissions: snap
                .permissions
                .into_iter()
                .map(Permission::from_snapshot)
                .collect(),
            description: snap.description,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct PermissionSnapshot {
    resource_type: ResourceTypeSnapshot,
    resource_identifier: ResourceIdentifierSnapshot,
    actions: ActionsSnapshot,
}

impl Snapshot for Permission {
    type Serialized = PermissionSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        PermissionSnapshot {
            resource_type: self.resource_type.snapshot(),
            resource_identifier: self.resource_identifier.snapshot(),
            actions: self.actions.snapshot(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            resource_type: ResourceType::from_snapshot(snap.resource_type),
            resource_identifier: ResourceIdentifier::from_snapshot(snap.resource_identifier),
            actions: Actions::from_snapshot(snap.actions),
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize)]
pub(crate) enum ResourceTypeSnapshot {
    Database,
    Token,
    Wildcard,
}

impl Snapshot for ResourceType {
    type Serialized = ResourceTypeSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            ResourceType::Database => ResourceTypeSnapshot::Database,
            ResourceType::Token => ResourceTypeSnapshot::Token,
            ResourceType::Wildcard => ResourceTypeSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ResourceTypeSnapshot::Database => ResourceType::Database,
            ResourceTypeSnapshot::Token => ResourceType::Token,
            ResourceTypeSnapshot::Wildcard => ResourceType::Wildcard,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) enum ResourceIdentifierSnapshot {
    Database(Vec<DbId>),
    Token(Vec<TokenId>),
    Wildcard,
}

impl Snapshot for ResourceIdentifier {
    type Serialized = ResourceIdentifierSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            ResourceIdentifier::Database(db_id) => {
                ResourceIdentifierSnapshot::Database(db_id.clone())
            }
            ResourceIdentifier::Token(token_id) => {
                ResourceIdentifierSnapshot::Token(token_id.clone())
            }
            ResourceIdentifier::Wildcard => ResourceIdentifierSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ResourceIdentifierSnapshot::Database(db_id) => ResourceIdentifier::Database(db_id),
            ResourceIdentifierSnapshot::Token(token_id) => ResourceIdentifier::Token(token_id),
            ResourceIdentifierSnapshot::Wildcard => ResourceIdentifier::Wildcard,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) enum ActionsSnapshot {
    Database(DatabaseActionsSnapshot),
    Token(CrudActionsSnapshot),
    Wildcard,
}

impl Snapshot for Actions {
    type Serialized = ActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        match self {
            Actions::Database(database_actions) => {
                ActionsSnapshot::Database(database_actions.snapshot())
            }
            Actions::Token(crud_actions) => ActionsSnapshot::Token(crud_actions.snapshot()),
            Actions::Wildcard => ActionsSnapshot::Wildcard,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        match snap {
            ActionsSnapshot::Database(db_actions) => {
                Actions::Database(DatabaseActions::from_snapshot(db_actions))
            }
            ActionsSnapshot::Token(crud_actions) => {
                Actions::Token(CrudActions::from_snapshot(crud_actions))
            }
            ActionsSnapshot::Wildcard => Actions::Wildcard,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct DatabaseActionsSnapshot(u16);

impl Snapshot for DatabaseActions {
    type Serialized = DatabaseActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        DatabaseActionsSnapshot(u16::MAX)
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        snap.0.into()
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub(crate) struct CrudActionsSnapshot(u16);

impl Snapshot for CrudActions {
    type Serialized = CrudActionsSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        CrudActionsSnapshot(u16::MAX)
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        snap.0.into()
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct NodeSnapshot {
    pub(crate) node_id: Arc<str>,
    pub(crate) node_catalog_id: NodeId,
    pub(crate) instance_id: Arc<str>,
    pub(crate) mode: Vec<NodeMode>,
    pub(crate) state: NodeState,
    pub(crate) core_count: u64,
}

impl Snapshot for NodeDefinition {
    type Serialized = NodeSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            node_id: Arc::clone(&self.node_id),
            node_catalog_id: self.node_catalog_id,
            instance_id: Arc::clone(&self.instance_id),
            mode: self.mode.clone(),
            state: self.state,
            core_count: self.core_count,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            node_id: snap.node_id,
            node_catalog_id: snap.node_catalog_id,
            instance_id: snap.instance_id,
            mode: snap.mode,
            core_count: snap.core_count,
            state: snap.state,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DatabaseSnapshot {
    pub(crate) id: DbId,
    pub(crate) name: Arc<str>,
    pub(crate) tables: RepositorySnapshot<TableId, TableSnapshot>,
    pub(crate) processing_engine_triggers:
        RepositorySnapshot<TriggerId, ProcessingEngineTriggerSnapshot>,
    pub(crate) deleted: bool,
}

impl Snapshot for DatabaseSchema {
    type Serialized = DatabaseSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            id: self.id,
            name: Arc::clone(&self.name),
            tables: self.tables.snapshot(),
            processing_engine_triggers: self.processing_engine_triggers.snapshot(),
            deleted: self.deleted,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            tables: Repository::from_snapshot(snap.tables),
            processing_engine_triggers: Repository::from_snapshot(snap.processing_engine_triggers),
            deleted: snap.deleted,
        }
    }
}

/// A snapshot of a [`TableDefinition`] used for serialization of table information from the
/// catalog.
///
/// This is used over serde's `Serialize`/`Deserialize` implementations on the inner `Schema` type
/// due to them being considered unstable. This type intends to mimic the structure of the Arrow
/// `Schema`, and will help guard against potential breaking changes to the Arrow Schema types.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct TableSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table_name: Arc<str>,
    pub(crate) key: Vec<ColumnId>,
    pub(crate) columns: RepositorySnapshot<ColumnId, ColumnDefinitionSnapshot>,
    pub(crate) last_caches: RepositorySnapshot<LastCacheId, LastCacheSnapshot>,
    pub(crate) distinct_caches: RepositorySnapshot<DistinctCacheId, DistinctCacheSnapshot>,
    pub(crate) deleted: bool,
}

impl Snapshot for TableDefinition {
    type Serialized = TableSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            table_id: self.table_id,
            table_name: Arc::clone(&self.table_name),
            key: self.series_key.clone(),
            columns: self.columns.snapshot(),
            last_caches: self.last_caches.snapshot(),
            distinct_caches: self.distinct_caches.snapshot(),
            deleted: self.deleted,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let table_id = snap.table_id;
        // use the TableDefinition constructor here since it handles
        // Schema construction:
        let mut table_def = Self::new(
            table_id,
            snap.table_name,
            snap.columns
                .repo
                .into_iter()
                .map(|(id, def)| {
                    (
                        id,
                        def.name,
                        match def.influx_type {
                            InfluxType::Tag => InfluxColumnType::Tag,
                            InfluxType::Field => {
                                InfluxColumnType::Field(InfluxFieldType::from(def.r#type))
                            }
                            InfluxType::Time => InfluxColumnType::Timestamp,
                        },
                    )
                })
                .collect(),
            snap.key,
        )
        .expect("serialized table definition from catalog should be valid");
        // ensure next col id is set from the snapshot incase we ever allow
        // hard-deletes:
        table_def.columns.set_next_id(snap.columns.next_id);
        Self {
            table_id,
            table_name: table_def.table_name,
            schema: table_def.schema,
            columns: table_def.columns,
            series_key: table_def.series_key,
            series_key_names: table_def.series_key_names,
            last_caches: Repository::from_snapshot(snap.last_caches),
            distinct_caches: Repository::from_snapshot(snap.distinct_caches),
            deleted: snap.deleted,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ProcessingEngineTriggerSnapshot {
    pub trigger_id: TriggerId,
    pub trigger_name: Arc<str>,
    pub node_id: Arc<str>,
    pub plugin_filename: String,
    pub database_name: Arc<str>,
    pub trigger_specification: TriggerSpecificationDefinition,
    pub trigger_settings: TriggerSettings,
    pub trigger_arguments: Option<HashMap<String, String>>,
    pub disabled: bool,
}

impl Snapshot for TriggerDefinition {
    type Serialized = ProcessingEngineTriggerSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            trigger_id: self.trigger_id,
            trigger_name: Arc::clone(&self.trigger_name),
            node_id: Arc::clone(&self.node_id),
            plugin_filename: self.plugin_filename.clone(),
            database_name: Arc::clone(&self.database_name),
            trigger_specification: self.trigger.clone(),
            trigger_settings: self.trigger_settings,
            trigger_arguments: self.trigger_arguments.clone(),
            disabled: self.disabled,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            trigger_id: snap.trigger_id,
            trigger_name: snap.trigger_name,
            node_id: snap.node_id,
            plugin_filename: snap.plugin_filename,
            database_name: snap.database_name,
            trigger: snap.trigger_specification,
            trigger_settings: snap.trigger_settings,
            trigger_arguments: snap.trigger_arguments,
            disabled: snap.disabled,
        }
    }
}

/// The inner column definition for a [`TableSnapshot`]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ColumnDefinitionSnapshot {
    pub(crate) name: Arc<str>,
    /// The id of the column
    pub(crate) id: ColumnId,
    /// The column's data type
    pub(crate) r#type: DataType,
    /// The columns Influx type
    pub(crate) influx_type: InfluxType,
    /// Whether the column can hold NULL values
    pub(crate) nullable: bool,
}

impl Snapshot for ColumnDefinition {
    type Serialized = ColumnDefinitionSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            name: Arc::clone(&self.name),
            id: self.id,
            r#type: self.data_type.into(),
            influx_type: self.data_type.into(),
            nullable: self.nullable,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            id: snap.id,
            name: snap.name,
            data_type: match snap.influx_type {
                InfluxType::Tag => InfluxColumnType::Tag,
                InfluxType::Field => InfluxColumnType::Field(InfluxFieldType::from(&snap.r#type)),
                InfluxType::Time => InfluxColumnType::Timestamp,
            },
            nullable: snap.nullable,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct LastCacheSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table: Arc<str>,
    pub(crate) id: LastCacheId,
    pub(crate) name: Arc<str>,
    pub(crate) keys: Vec<ColumnId>,
    pub(crate) vals: Option<Vec<ColumnId>>,
    pub(crate) n: usize,
    pub(crate) ttl: u64,
}

impl Snapshot for LastCacheDefinition {
    type Serialized = LastCacheSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            table_id: self.table_id,
            table: Arc::clone(&self.table),
            id: self.id,
            name: Arc::clone(&self.name),
            keys: self.key_columns.to_vec(),
            vals: match &self.value_columns {
                LastCacheValueColumnsDef::Explicit { columns } => Some(columns.to_vec()),
                LastCacheValueColumnsDef::AllNonKeyColumns => None,
            },
            n: self.count.into(),
            ttl: self.ttl.as_secs(),
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            table_id: snap.table_id,
            table: snap.table,
            id: snap.id,
            name: snap.name,
            key_columns: snap.keys,
            value_columns: snap.vals.map_or_else(Default::default, |columns| {
                LastCacheValueColumnsDef::Explicit { columns }
            }),
            count: snap
                .n
                .try_into()
                .expect("catalog contains invalid last cache size"),
            ttl: LastCacheTtl::from_secs(snap.ttl),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DistinctCacheSnapshot {
    pub(crate) table_id: TableId,
    pub(crate) table: Arc<str>,
    pub(crate) id: DistinctCacheId,
    pub(crate) name: Arc<str>,
    pub(crate) cols: Vec<ColumnId>,
    pub(crate) max_cardinality: MaxCardinality,
    pub(crate) max_age_seconds: MaxAge,
}

impl Snapshot for DistinctCacheDefinition {
    type Serialized = DistinctCacheSnapshot;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            table_id: self.table_id,
            table: Arc::clone(&self.table_name),
            id: self.cache_id,
            name: Arc::clone(&self.cache_name),
            cols: self.column_ids.clone(),
            max_cardinality: self.max_cardinality,
            max_age_seconds: self.max_age_seconds,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        Self {
            table_id: snap.table_id,
            table_name: snap.table,
            cache_id: snap.id,
            cache_name: snap.name,
            column_ids: snap.cols,
            max_cardinality: snap.max_cardinality,
            max_age_seconds: snap.max_age_seconds,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub(crate) struct RepositorySnapshot<I, R>
where
    I: CatalogId,
{
    pub(crate) repo: SerdeVecMap<I, R>,
    pub(crate) next_id: I,
}

impl<I, R> Snapshot for Repository<I, R>
where
    I: CatalogId,
    R: Snapshot + CatalogResource,
{
    type Serialized = RepositorySnapshot<I, R::Serialized>;

    fn snapshot(&self) -> Self::Serialized {
        Self::Serialized {
            repo: self
                .repo
                .iter()
                .map(|(id, res)| (*id, res.snapshot()))
                .collect(),
            next_id: self.next_id,
        }
    }

    fn from_snapshot(snap: Self::Serialized) -> Self {
        let mut repo = Self::new();
        for (id, res) in snap.repo {
            repo.insert(id, Arc::new(R::from_snapshot(res)))
                .expect("catalog should contain no duplicates");
        }
        Self {
            id_name_map: repo.id_name_map,
            repo: repo.repo,
            next_id: snap.next_id,
        }
    }
}

/// Representation of Arrow's `DataType` for table snapshots.
///
/// Uses `#[non_exhaustive]` with the assumption that variants will be added as we support
/// more Arrow data types.
#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub(crate) enum DataType {
    Null,
    Bool,
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
    F16,
    F32,
    F64,
    Str,
    BigStr,
    StrView,
    Bin,
    BigBin,
    BinView,
    Dict(Box<DataType>, Box<DataType>),
    Time(TimeUnit, Option<Arc<str>>),
}

/// Representation of Arrow's `TimeUnit` for table snapshots.
#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub(crate) enum TimeUnit {
    #[serde(rename = "s")]
    Second,
    #[serde(rename = "ms")]
    Millisecond,
    #[serde(rename = "us")]
    Microsecond,
    #[serde(rename = "ns")]
    Nanosecond,
}

impl From<arrow::datatypes::TimeUnit> for TimeUnit {
    fn from(arrow_unit: arrow::datatypes::TimeUnit) -> Self {
        match arrow_unit {
            arrow::datatypes::TimeUnit::Second => Self::Second,
            arrow::datatypes::TimeUnit::Millisecond => Self::Millisecond,
            arrow::datatypes::TimeUnit::Microsecond => Self::Microsecond,
            arrow::datatypes::TimeUnit::Nanosecond => Self::Nanosecond,
        }
    }
}

/// Used to annotate columns in a Schema by their respective type in the Influx Data Model
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum InfluxType {
    Tag,
    Field,
    Time,
}

impl From<InfluxColumnType> for InfluxType {
    fn from(col_type: InfluxColumnType) -> Self {
        match col_type {
            InfluxColumnType::Tag => Self::Tag,
            InfluxColumnType::Field(_) => Self::Field,
            InfluxColumnType::Timestamp => Self::Time,
        }
    }
}

impl From<InfluxColumnType> for DataType {
    fn from(value: InfluxColumnType) -> Self {
        match value {
            InfluxColumnType::Tag => Self::Dict(Box::new(Self::I32), Box::new(Self::Str)),
            InfluxColumnType::Field(field) => match field {
                InfluxFieldType::Float => Self::F64,
                InfluxFieldType::Integer => Self::I64,
                InfluxFieldType::UInteger => Self::U64,
                InfluxFieldType::String => Self::Str,
                InfluxFieldType::Boolean => Self::Bool,
            },
            InfluxColumnType::Timestamp => Self::Time(TimeUnit::Nanosecond, TIME_DATA_TIMEZONE()),
        }
    }
}

impl From<&ArrowDataType> for DataType {
    fn from(arrow_type: &ArrowDataType) -> Self {
        match arrow_type {
            ArrowDataType::Null => Self::Null,
            ArrowDataType::Boolean => Self::Bool,
            ArrowDataType::Int8 => Self::I8,
            ArrowDataType::Int16 => Self::I16,
            ArrowDataType::Int32 => Self::I32,
            ArrowDataType::Int64 => Self::I64,
            ArrowDataType::UInt8 => Self::U8,
            ArrowDataType::UInt16 => Self::U16,
            ArrowDataType::UInt32 => Self::U32,
            ArrowDataType::UInt64 => Self::U64,
            ArrowDataType::Float16 => Self::F16,
            ArrowDataType::Float32 => Self::F32,
            ArrowDataType::Float64 => Self::F64,
            ArrowDataType::Timestamp(unit, tz) => Self::Time((*unit).into(), tz.clone()),
            ArrowDataType::Date32 => unimplemented!(),
            ArrowDataType::Date64 => unimplemented!(),
            ArrowDataType::Time32(_) => unimplemented!(),
            ArrowDataType::Time64(_) => unimplemented!(),
            ArrowDataType::Duration(_) => unimplemented!(),
            ArrowDataType::Interval(_) => unimplemented!(),
            ArrowDataType::Binary => Self::Bin,
            ArrowDataType::FixedSizeBinary(_) => unimplemented!(),
            ArrowDataType::LargeBinary => Self::BigBin,
            ArrowDataType::BinaryView => Self::BinView,
            ArrowDataType::Utf8 => Self::Str,
            ArrowDataType::LargeUtf8 => Self::BigStr,
            ArrowDataType::Utf8View => Self::StrView,
            ArrowDataType::List(_) => unimplemented!(),
            ArrowDataType::ListView(_) => unimplemented!(),
            ArrowDataType::FixedSizeList(_, _) => unimplemented!(),
            ArrowDataType::LargeList(_) => unimplemented!(),
            ArrowDataType::LargeListView(_) => unimplemented!(),
            ArrowDataType::Struct(_) => unimplemented!(),
            ArrowDataType::Union(_, _) => unimplemented!(),
            ArrowDataType::Dictionary(key_type, val_type) => Self::Dict(
                Box::new(key_type.as_ref().into()),
                Box::new(val_type.as_ref().into()),
            ),
            ArrowDataType::Decimal128(_, _) => unimplemented!(),
            ArrowDataType::Decimal256(_, _) => unimplemented!(),
            ArrowDataType::Map(_, _) => unimplemented!(),
            ArrowDataType::RunEndEncoded(_, _) => unimplemented!(),
        }
    }
}

// NOTE: Ideally, we will remove the need for the InfluxFieldType, and be able
// to use Arrow's DataType directly. If that happens, this conversion will need
// to support the entirety of Arrow's DataType enum, which is why [`DataType`]
// has been defined to mimic the Arrow type.
//
// See <https://github.com/influxdata/influxdb_iox/issues/11111>
impl From<DataType> for InfluxFieldType {
    fn from(data_type: DataType) -> Self {
        match data_type {
            DataType::Bool => Self::Boolean,
            DataType::I64 => Self::Integer,
            DataType::U64 => Self::UInteger,
            DataType::F64 => Self::Float,
            DataType::Str => Self::String,
            other => unimplemented!("unsupported data type in catalog {other:?}"),
        }
    }
}

impl From<&DataType> for InfluxFieldType {
    fn from(data_type: &DataType) -> Self {
        match data_type {
            DataType::Bool => Self::Boolean,
            DataType::I64 => Self::Integer,
            DataType::U64 => Self::UInteger,
            DataType::F64 => Self::Float,
            DataType::Str => Self::String,
            other => unimplemented!("unsupported data type in catalog {other:?}"),
        }
    }
}
